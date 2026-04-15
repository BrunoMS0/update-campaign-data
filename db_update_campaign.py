"""
actualizar_bd.py
Actualiza 4 tablas en BD a partir del Excel de búsqueda.

Columnas esperadas en el Excel:
    ID_POINTER, ID_VENDEDOR, GRUPO, WEBSITE, CONTACTO_1, CARGO_1,
    CODIGO_ESTADO, EMAIL_1, EMAIL_2, EMAIL_3, EMAIL_4,
    TELF_1, TELF_2, TELF_3, OCURRENCIA, FECHA_OCURRENCIA

Dependencias:
    pip install pandas openpyxl pymysql

Uso:
    python actualizar_bd.py
"""

import re
import sys
import os
import pandas as pd
import pymysql
from pymysql.constants import CLIENT
import logging
from datetime import datetime
from dotenv import load_dotenv

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────
load_dotenv()  # lee el archivo .env de la misma carpeta

def _require_env(key: str) -> str:
    """Lee una variable de entorno. Aborta si no está definida."""
    v = os.getenv(key)
    if not v:
        raise EnvironmentError(
            f"Variable de entorno '{key}' no definida. "
            "Revisa que el archivo .env exista y tenga el formato correcto."
        )
    return v

DB_CONFIG = {
    "host":     _require_env("DB_HOST"),
    "port":     int(os.getenv("DB_PORT", "3306")),
    "user":     _require_env("DB_USER"),
    "password": _require_env("DB_PASSWORD"),
    "db":       _require_env("DB_NAME"),
    "charset":  "utf8mb4",
}

EXCEL_PATH = "update.csv"

ID_EMPRESA         = "02"
ID_CAMPANA         = "18010003"
ID_TIPO_OCURRENCIA = "V"
ID_TIPO_ENTIDAD    = "P"
TEL_BASE           = 49001

EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')

# ─── LOGGING ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(
            open(sys.stdout.fileno(), mode='w', encoding='utf-8', closefd=False)
        ),
        logging.FileHandler(
            f"actualizacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)


# ─── HELPERS ──────────────────────────────────────────────────────────────────
def val(x):
    """Convierte NaN/NaT a None y limpia strings."""
    if pd.isna(x) or x is None:
        return None
    s = str(x).strip()
    return s if s else None


def normalizar_tel(t: str) -> str:
    """Extrae solo dígitos para comparación neutral (ignora espacios, guiones, +)."""
    return "".join(filter(str.isdigit, t)) if t else ""


def email_valido(email: str) -> bool:
    return bool(EMAIL_RE.match(email))


# ─── VALIDACIONES PRE-EJECUCIÓN ───────────────────────────────────────────────
def validar_excel(df: pd.DataFrame) -> bool:
    """
    Verifica integridad del Excel antes de iniciar cualquier operación en BD.
    Si hay duplicados en ID_POINTER: aborta con mensaje explícito y retorna False.
    """
    ids = df["ID_POINTER"].dropna().str.strip()
    duplicados = ids[ids.duplicated()].unique().tolist()
    if duplicados:
        log.error("=" * 60)
        log.error("EJECUCIÓN ABORTADA — IDs DUPLICADOS EN EL EXCEL")
        log.error("=" * 60)
        log.error("Se encontraron los siguientes ID_POINTER duplicados:")
        for d in duplicados:
            filas = df.index[df["ID_POINTER"].str.strip() == d].tolist()
            filas_excel = [f + 2 for f in filas]
            log.error(f"  ID {d} aparece en filas de Excel: {filas_excel}")
        log.error("")
        log.error("Corrija los duplicados en el Excel y vuelva a ejecutar.")
        log.error("=" * 60)
        return False
    return True


def validar_codigos_estado(cur, df: pd.DataFrame) -> bool:
    """
    Verifica que todos los CODIGO_ESTADO no vacíos del Excel existan en la tabla datos.
    Las filas con CODIGO_ESTADO vacío se omiten de esta validación (el UPDATE las saltará).
    Si algún código no existe: aborta con mensaje explícito y retorna False.
    """
    codigos = (
        df["CODIGO_ESTADO"]
        .dropna()
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    if not codigos:
        log.warning("  Ningún CODIGO_ESTADO presente en el Excel — validación omitida")
        return True

    placeholders = ",".join(["%s"] * len(codigos))
    cur.execute(
        f"SELECT idDato FROM datos WHERE idDato IN ({placeholders})",
        codigos,
    )
    encontrados = {r[0].strip() for r in cur.fetchall()}
    invalidos = [c for c in codigos if c not in encontrados]
    if invalidos:
        log.error("=" * 60)
        log.error("EJECUCIÓN ABORTADA — CÓDIGOS DE ESTADO INVÁLIDOS")
        log.error("=" * 60)
        log.error("Los siguientes CODIGO_ESTADO no existen en la tabla datos:")
        for c in invalidos:
            filas = df.index[df["CODIGO_ESTADO"].str.strip() == c].tolist()
            filas_excel = [f + 2 for f in filas]
            log.error(f"  Código '{c}' → filas: {filas_excel}")
        log.error("")
        log.error("Corrija los códigos en el Excel y vuelva a ejecutar.")
        log.error("=" * 60)
        return False
    return True


# ─── GENERACIÓN DE ID OCURRENCIA (atómica con bloqueo) ───────────────────────
def get_next_ocurrencia_id(cur) -> str:
    """
    Obtiene el siguiente idOcurrencia de forma atómica usando SELECT FOR UPDATE.
    Bloquea la lectura del MAX mientras se calcula el siguiente ID,
    evitando race conditions si el script se ejecuta en paralelo.
    """
    cur.execute(
        "SELECT MAX(CAST(idOcurrencia AS UNSIGNED)) FROM ocurrencias "
        "WHERE idEmpresa = %s FOR UPDATE",
        (ID_EMPRESA,),
    )
    row = cur.fetchone()
    last = row[0] if row and row[0] else 0
    return str(last + 1).zfill(8)


def get_next_nsec(cur, id_pc: str) -> int:
    """Retorna MAX(nsec)+1 si la entidad ya tiene ocurrencias, o 1 si es nueva."""
    cur.execute(
        "SELECT MAX(CAST(nsec AS UNSIGNED)) FROM ocurrencias "
        "WHERE idEntidad = %s AND idEmpresa = %s",
        (id_pc, ID_EMPRESA),
    )
    row = cur.fetchone()
    last = row[0] if row and row[0] is not None else 0
    return last + 1


# ─── MÓDULOS POR TABLA ────────────────────────────────────────────────────────

def update_precontactos(cur, id_pc: str, row: pd.Series) -> bool:
    """
    UPDATE 100% dinámico sobre la tabla precontactos.
    Evalúa cada campo individualmente: solo se incluyen en el SET
    los campos que tienen valor en el Excel.
    Ningún campo vacío sobreescribe datos existentes en BD.

    Campos actualizados (si tienen valor):
        ID_VENDEDOR  → idVendedor
        GRUPO        → idPreContactoGrupo
        WEBSITE      → paginaWEB
        CODIGO_ESTADO→ idEstadoSeguimiento
        CONTACTO_1   → GlsRepresentante
        CARGO_1      → GlsCargoRepresentante

    Retorna False si el ID no existe en BD (rowcount == 0).
    Si ningún campo tiene valor, omite el UPDATE y continúa el flujo.
    """
    # Mapa ordenado: columna Excel → columna BD
    candidatos = {
        "ID_VENDEDOR":   "idVendedor",
        "GRUPO":         "idPreContactoGrupo",
        "WEBSITE":       "paginaWEB",
        "CODIGO_ESTADO": "idEstadoSeguimiento",
        "CONTACTO_1":    "GlsRepresentante",
        "CARGO_1":       "GlsCargoRepresentante",
    }

    campos = {}
    for col_excel, col_bd in candidatos.items():
        v = val(row[col_excel])
        if v:
            campos[col_bd] = v
        else:
            log.warning(f"  {col_excel} vacío — {col_bd} no se actualizará")

    if not campos:
        log.warning(f"  Ningún campo con valor para {id_pc} — UPDATE omitido, flujo continúa")
        return True

    set_clause = ", ".join(f"{col} = %s" for col in campos)
    valores    = list(campos.values()) + [id_pc]
    cur.execute(
        f"UPDATE precontactos SET {set_clause} WHERE idPreContacto = %s",
        valores,
    )
    if cur.rowcount == 0:
        log.error(f"  ID_POINTER {id_pc} no existe en precontactos — fila abortada")
        return False
    return True


def sync_emails(cur, id_pc: str, row: pd.Series):
    """
    Reconciliación por conjunto (set-based) sobre precontactosemail.
    - Compara por VALOR (no por posición/item).
    - Solo inserta emails del Excel que no existen en BD.
    - Valida formato email con regex antes de insertar.
    - Nunca sobreescribe ni elimina: conserva historial completo.
    - El item nuevo = MAX(item existente) + 1.
    """
    cur.execute(
        "SELECT item, Email FROM precontactosemail WHERE idPreContacto = %s ORDER BY item",
        (id_pc,),
    )
    bd_rows   = cur.fetchall()
    bd_emails = {r[1].strip().lower() for r in bd_rows if r[1]}
    next_item = (max(r[0] for r in bd_rows) + 1) if bd_rows else 1

    for col in ["EMAIL_1", "EMAIL_2", "EMAIL_3", "EMAIL_4"]:
        email = val(row[col])
        if not email:
            continue
        if not email_valido(email):
            log.warning(f"  {col} formato inválido, omitido: '{email}'")
            continue
        if email.lower() in bd_emails:
            log.debug(f"  {col} ya existe en BD, omitido: {email}")
            continue
        cur.execute(
            "INSERT INTO precontactosemail (idPreContacto, item, Email) VALUES (%s, %s, %s)",
            (id_pc, next_item, email),
        )
        log.debug(f"  Email item {next_item} → INSERT ({email})")
        bd_emails.add(email.lower())
        next_item += 1


def sync_telefonos(cur, id_pc: str, row: pd.Series):
    """
    Reconciliación por conjunto (set-based) sobre precontactostelefonos.
    - Compara por dígitos normalizados (ignora +, espacios, guiones).
    - Solo inserta teléfonos del Excel que no existen en BD.
    - Inserta el valor tal cual viene del Excel (sin modificar formato).
    - Sin límite en IdEmpresaTelefonica (puede superar 49005).
    - Nunca sobreescribe ni elimina: conserva historial completo.
    - El IdEmpresaTelefonica nuevo = MAX(id existente) + 1.
    """
    cur.execute(
        "SELECT IdEmpresaTelefonica, Telefono FROM precontactostelefonos "
        "WHERE idPreContacto = %s",
        (id_pc,),
    )
    bd_rows     = cur.fetchall()
    bd_tels     = {normalizar_tel(r[1]) for r in bd_rows if r[1]}
    next_id_tel = (max(int(r[0]) for r in bd_rows) + 1) if bd_rows else TEL_BASE

    for col in ["TELF_1", "TELF_2", "TELF_3"]:
        telefono = val(row[col])
        if not telefono:
            continue
        if normalizar_tel(telefono) in bd_tels:
            log.debug(f"  {col} ya existe en BD, omitido: {telefono}")
            continue
        cur.execute(
            "INSERT INTO precontactostelefonos "
            "(idPreContacto, IdEmpresaTelefonica, Telefono) VALUES (%s, %s, %s)",
            (id_pc, str(next_id_tel), telefono),
        )
        log.debug(f"  Telf {next_id_tel} → INSERT ({telefono})")
        bd_tels.add(normalizar_tel(telefono))
        next_id_tel += 1


def insert_ocurrencia(cur, id_pc: str, row: pd.Series) -> str | None:
    """
    INSERT en ocurrencias con dos guards:
    1. Si OCURRENCIA es NULL/vacío → no se inserta (aunque haya fecha).
    2. Si ya existe una ocurrencia con la misma fecha e idEntidad → no se inserta
       (guard de idempotencia ante re-ejecuciones accidentales).
    Usa SELECT FOR UPDATE para calcular idOcurrencia de forma atómica.
    Retorna el idOcurrencia insertado, o None si se omitió.
    """
    glsOc     = val(row["OCURRENCIA"])
    fecha_raw = val(row["FECHA_OCURRENCIA"])

    # Guard 1: sin texto de ocurrencia, no insertar aunque haya fecha
    if not glsOc:
        log.warning(f"  OCURRENCIA vacía — no se insertará ocurrencia para {id_pc}")
        return None

    fecha_oc = pd.to_datetime(fecha_raw, dayfirst=True).strftime("%Y-%m-%d %H:%M:%S") if fecha_raw else None

    # Guard 2: idempotencia — verificar si ya existe misma fecha + entidad
    if fecha_oc:
        cur.execute(
            "SELECT idOcurrencia FROM ocurrencias "
            "WHERE idEntidad = %s AND idEmpresa = %s AND DATE(FechaOcurrencia) = DATE(%s)",
            (id_pc, ID_EMPRESA, fecha_oc),
        )
        existente = cur.fetchone()
        if existente:
            log.warning(
                f"  Ocurrencia ya existe para {id_pc} con fecha {fecha_oc[:10]} "
                f"(id={existente[0]}) — se omite para evitar duplicado"
            )
            return None

    # SELECT FOR UPDATE: bloqueo atómico para evitar race condition en idOcurrencia
    id_ocurrencia = get_next_ocurrencia_id(cur)
    nsec          = get_next_nsec(cur, id_pc)

    cur.execute(
        """
        INSERT INTO ocurrencias
            (idEmpresa, idOcurrencia, idTipoOcurrencia, idTipoEntidad,
             FechaOcurrencia, GlsOcurrencia, idEntidad,
             idEstadoPreContacto, idCampana, nsec)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            ID_EMPRESA, id_ocurrencia, ID_TIPO_OCURRENCIA, ID_TIPO_ENTIDAD,
            fecha_oc, glsOc, id_pc,
            val(row["CODIGO_ESTADO"]) or "", ID_CAMPANA, str(nsec),
        ),
    )
    log.debug(f"  nsec={nsec} ({'nueva entidad' if nsec == 1 else 'existente'})")
    return id_ocurrencia


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    # ── 1. Cargar Excel ───────────────────────────────────────────────────────
    df = pd.read_csv(EXCEL_PATH, dtype=str, encoding="utf-8")
    df.columns = df.columns.str.strip()
    df["ID_POINTER"] = df["ID_POINTER"].str.strip()
    log.info(f"Excel cargado: {len(df)} filas")

    # ── 2. Validación 1: duplicados en Excel (sin tocar BD) ───────────────────
    if not validar_excel(df):
        sys.exit(1)

    ok, omitidos, errores = 0, [], []

    conn = pymysql.connect(**DB_CONFIG, autocommit=False,
                            client_flag=CLIENT.FOUND_ROWS)
    try:
        with conn.cursor() as cur:

            # ── 3. Validación 2: códigos de estado contra BD ──────────────────
            if not validar_codigos_estado(cur, df):
                sys.exit(1)

            # ── 4. Loop principal ─────────────────────────────────────────────
            for idx, row in df.iterrows():
                id_pc = val(row.get("ID_POINTER"))
                fila  = idx + 2
                if not id_pc:
                    log.warning(f"Fila {fila}: sin ID_POINTER, omitida")
                    continue

                log.info(f"[Fila {fila}] ID_POINTER: {id_pc}")
                try:
                    # 4a. precontactos UPDATE dinámico
                    if not update_precontactos(cur, id_pc, row):
                        conn.rollback()
                        errores.append({
                            "fila": fila, "id": id_pc,
                            "error": "ID_POINTER no encontrado en precontactos",
                        })
                        continue

                    # 4b. emails sync (set-based)
                    sync_emails(cur, id_pc, row)

                    # 4c. teléfonos sync (set-based)
                    sync_telefonos(cur, id_pc, row)

                    # 4d. ocurrencia INSERT (con guards de nulidad e idempotencia)
                    id_oc = insert_ocurrencia(cur, id_pc, row)
                    if id_oc:
                        log.info(f"  Ocurrencia insertada: {id_oc}")
                    else:
                        omitidos.append({"fila": fila, "id": id_pc})

                    conn.commit()
                    ok += 1

                except Exception as e:
                    conn.rollback()
                    log.error(f"  ERROR fila {fila} (ID={id_pc}): {e}")
                    errores.append({"fila": fila, "id": id_pc, "error": str(e)})

    finally:
        conn.close()

    # ── 5. Resumen final ──────────────────────────────────────────────────────
    log.info("=" * 60)
    log.info(f"COMPLETADO: {ok} filas OK | {len(errores)} errores | "
             f"{len(omitidos)} ocurrencias omitidas")
    if omitidos:
        log.info("Ocurrencias omitidas (OCURRENCIA vacía o ya existente):")
        for o in omitidos:
            log.info(f"  Fila {o['fila']} | ID {o['id']}")
    if errores:
        log.warning("Filas con error:")
        for e in errores:
            log.warning(f"  Fila {e['fila']} | ID {e['id']} → {e['error']}")
        log.info("")
        log.info("RESUMEN DE FALLOS:")
        log.info(f"{'ID_POINTER':<12} | ERROR")
        log.info("-" * 60)
        for e in errores:
            log.info(f"{e['id']:<12} | {e['error']}")


if __name__ == "__main__":
    main()