# actualizar_bd.py

Script de actualización batch que lee un archivo Excel y sincroniza los datos en 4 tablas de la base de datos: `precontactos`, `precontactosemail`, `precontactostelefonos` y `ocurrencias`.

---

## Requisitos

- Python 3.10 o superior
- Las siguientes librerías:

```bash
pip install pandas openpyxl pymysql python-dotenv
```

---

## Configuración de credenciales

Crea un archivo `.env` en la misma carpeta que el script con el siguiente contenido:

```env
DB_HOST=tu_host
DB_PORT=3306
DB_USER=tu_usuario
DB_PASSWORD=tu_password
DB_NAME=tu_base_de_datos
```

> **Importante:** nunca subas el archivo `.env` a un repositorio. Agrégalo a tu `.gitignore`.

---

## Formato del Excel

El archivo Excel debe tener exactamente las siguientes columnas con estos nombres (sensible a mayúsculas y espacios):

| Columna | Obligatorio | Descripción |
|---|---|---|
| `ID_POINTER` | ✅ Sí | Identificador único de la empresa. Se usa para hacer match en BD. |
| `ID_VENDEDOR` | No | ID del ejecutivo de ventas asignado. |
| `GRUPO` | No | Grupo o segmento al que pertenece el precontacto. |
| `WEBSITE` | No | URL del sitio web de la empresa. |
| `CONTACTO_1` | No | Nombre del representante principal. |
| `CARGO_1` | No | Cargo del representante principal. |
| `CODIGO_ESTADO` | No | Código del estado de seguimiento. Debe existir en la tabla `datos` de BD. |
| `EMAIL_1` | No | Primer email de contacto. |
| `EMAIL_2` | No | Segundo email de contacto. |
| `EMAIL_3` | No | Tercer email de contacto. |
| `EMAIL_4` | No | Cuarto email de contacto. |
| `TELF_1` | No | Primer teléfono de contacto. |
| `TELF_2` | No | Segundo teléfono de contacto. |
| `TELF_3` | No | Tercer teléfono de contacto. |
| `OCURRENCIA` | No | Texto descriptivo de la interacción realizada. |
| `FECHA_OCURRENCIA` | No | Fecha de la ocurrencia. Formato esperado: `YYYY-MM-DD` o `YYYY-MM-DD HH:MM:SS`. |

> **Regla general de campos vacíos:** si una celda está vacía, ese campo se omite completamente. No se sobreescribe ningún dato existente en BD con valores nulos.

---

## Ejecución

```bash
python actualizar_bd.py
```

Al terminar, se genera automáticamente un archivo de log con el nombre `actualizacion_YYYYMMDD_HHMMSS.log` en la misma carpeta.

---

## Qué hace el script

### Validaciones previas (antes de tocar la BD)

El script ejecuta dos validaciones al inicio. Si alguna falla, **aborta completamente** sin modificar nada en la base de datos:

1. **Duplicados en Excel:** verifica que no haya dos filas con el mismo `ID_POINTER`. Si los hay, muestra exactamente en qué filas están y detiene la ejecución.
2. **Códigos de estado:** verifica que todos los valores no vacíos de `CODIGO_ESTADO` existan en la tabla `datos` de BD. Si alguno no existe, muestra cuáles son inválidos y detiene la ejecución.

---

### Procesamiento fila por fila

Por cada fila del Excel, el script ejecuta hasta 4 operaciones dentro de una **transacción individual**. Si alguna operación falla, se hace rollback solo de esa fila y el script continúa con la siguiente.

#### 1. `precontactos` — UPDATE dinámico

Busca el registro por `ID_POINTER` → `idPreContacto` y actualiza únicamente los campos que tienen valor en el Excel:

| Excel | BD |
|---|---|
| `ID_VENDEDOR` | `idVendedor` |
| `GRUPO` | `idPreContactoGrupo` |
| `WEBSITE` | `paginaWEB` |
| `CODIGO_ESTADO` | `idEstadoSeguimiento` |
| `CONTACTO_1` | `GlsRepresentante` |
| `CARGO_1` | `GlsCargoRepresentante` |

- Si el `ID_POINTER` no existe en BD → la fila completa se aborta (no se insertan emails, teléfonos ni ocurrencia).
- Si todos los campos están vacíos → el UPDATE se omite y el flujo continúa normalmente hacia emails, teléfonos y ocurrencia.

#### 2. `precontactosemail` — sincronización por conjunto

Lógica set-based: compara los emails del Excel contra los existentes en BD **por valor**, no por posición.

- Si el email ya existe en BD → se omite (no hay duplicados).
- Si el email es nuevo → se inserta con `item = MAX(item existente) + 1`.
- Si el email tiene formato inválido (sin `@`, sin dominio) → se omite con advertencia en el log.
- Los emails existentes en BD nunca se eliminan ni sobreescriben.

#### 3. `precontactostelefonos` — sincronización por conjunto

Misma lógica set-based que emails, pero con normalización de formato: antes de comparar, se extraen solo los dígitos del número (ignora `+`, espacios, guiones). Así `+51 999-123` y `51999123` se reconocen como el mismo número.

- Si el teléfono ya existe en BD (normalizado) → se omite.
- Si es nuevo → se inserta con `IdEmpresaTelefonica = MAX(id existente) + 1`.
- El valor se guarda tal cual viene del Excel, sin modificar el formato.
- Los teléfonos existentes en BD nunca se eliminan ni sobreescriben.

#### 4. `ocurrencias` — INSERT con guards

Se inserta una nueva ocurrencia con los siguientes datos fijos:

| Campo | Valor |
|---|---|
| `idEmpresa` | `02` |
| `idTipoOcurrencia` | `V` |
| `idTipoEntidad` | `P` |
| `idCampana` | `18010003` |
| `nsec` | `MAX(nsec) + 1` para esa entidad, o `1` si es su primera ocurrencia |

Dos guards evitan inserciones incorrectas:

1. **Ocurrencia vacía:** si `OCURRENCIA` no tiene texto → no se inserta nada, aunque `FECHA_OCURRENCIA` tenga valor.
2. **Idempotencia:** si ya existe una ocurrencia con la misma `FECHA_OCURRENCIA` e `idEntidad` → no se inserta de nuevo. Esto protege ante re-ejecuciones accidentales del script con el mismo Excel.

El `idOcurrencia` se calcula con `SELECT MAX() FOR UPDATE` para evitar colisiones si el script se ejecuta en paralelo.

---

## Estructura del log

Cada ejecución genera un log con el siguiente formato:

```
2026-03-26 10:00:01 [INFO]    Excel cargado: 109 filas
2026-03-26 10:00:01 [INFO]    [Fila 2] ID_POINTER: 80239919
2026-03-26 10:00:01 [WARNING]   CARGO_1 vacío — GlsCargoRepresentante no se actualizará
2026-03-26 10:00:01 [INFO]      Ocurrencia insertada: 00001501
2026-03-26 10:00:02 [ERROR]   ERROR fila 5 (ID=80239926): ...
2026-03-26 10:00:05 [INFO]    ============================================================
2026-03-26 10:00:05 [INFO]    COMPLETADO: 107 filas OK | 1 errores | 6 ocurrencias omitidas
```

---

## Casos que generan advertencia (no detienen el flujo)

| Situación | Comportamiento |
|---|---|
| Campo vacío en `WEBSITE`, `CODIGO_ESTADO`, `CONTACTO_1`, `CARGO_1`, `ID_VENDEDOR` o `GRUPO` | Se omite ese campo, el resto se procesa normalmente |
| Email con formato inválido | Se omite ese email, se continúa con el siguiente |
| Teléfono ya existente en BD | Se omite, no se duplica |
| Email ya existente en BD | Se omite, no se duplica |
| `OCURRENCIA` vacía | No se inserta ocurrencia para esa empresa |
| Ocurrencia ya existente con misma fecha | No se inserta de nuevo (idempotencia) |

## Casos que abortan la fila completa

| Situación | Comportamiento |
|---|---|
| `ID_POINTER` no existe en `precontactos` | Se hace rollback de la fila, se registra el error y se continúa con la siguiente |
| Error inesperado de BD en cualquier operación | Ídem |

## Casos que abortan toda la ejecución

| Situación | Comportamiento |
|---|---|
| `ID_POINTER` duplicado en el Excel | El script no toca la BD y termina con mensaje de error |
| `CODIGO_ESTADO` no existe en tabla `datos` | El script no toca la BD y termina con mensaje de error |