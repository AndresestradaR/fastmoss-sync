# FastMoss Sync

Servicio de sincronización de productos de TikTok Shop (via FastMoss API) a Supabase. Diseñado para ejecutarse como cron job en Railway.

## Configuración

### 1. Crear tabla en Supabase

Ejecuta el contenido de `schema.sql` en el SQL Editor de Supabase.

### 2. Variables de entorno (Railway)

```env
SUPABASE_URL=https://xxx.supabase.co
SUPABASE_KEY=tu_service_role_key
FASTMOSS_REGIONS=US,MX,BR
FASTMOSS_CATEGORIES=14,25,9,16
SYNC_LIMIT_PER_REGION=500
```

### Categorías disponibles

| ID | Categoría |
|----|-----------|
| 14 | Belleza |
| 25 | Salud |
| 9  | Deportes |
| 16 | Electrónica |

### Regiones disponibles

US, MX, BR, ES, GB, DE, FR, IT, ID, VN, TH, MY, PH, JP, SG

## Deploy en Railway

1. Conecta este repo a Railway
2. Configura las variables de entorno
3. El cron se ejecutará diariamente a las 6:00 AM UTC

## Ejecución local

```bash
# Instalar dependencias
pip install -r requirements.txt

# Crear archivo .env con las variables
cp .env.example .env

# Ejecutar
python main.py
```

## Estructura

```
fastmoss-sync/
├── main.py              # Entry point
├── sync.py              # Lógica de sincronización
├── fastmoss_client.py   # Cliente API FastMoss
├── config.py            # Configuración
├── requirements.txt
├── railway.toml         # Config Railway con cron
├── Procfile
├── schema.sql           # SQL para Supabase
└── README.md
```
