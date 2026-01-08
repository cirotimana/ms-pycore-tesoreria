# Auth Module

modulo de autenticacion jwt para el proyecto pycore-backend api

## inicio rapido

### 1. configurar variables de entorno

agregar al archivo `.env`:

```bash
jwt_secret_key=tu_clave_secreta_aqui
jwt_algorithm=hs256
jwt_expiration_time=86400
```

### 2. generar hash de contraseña

```bash
python -m app.auth.hash_password
```

### 3. crear usuario en la base de datos

```sql
insert into users (
    first_name,
    last_name,
    email,
    password,
    username,
    is_active
) values (
    'juan',
    'perez',
    'juan@example.com',
    'hash_generado_con_el_script',
    'juanperez',
    true
);
```

### 4. hacer login

```bash
curl -x post http://localhost:8000/auth/login \
  -h "content-type: application/json" \
  -d '{"username": "juanperez", "password": "tu_contraseña"}'
```

### 5. usar el token

```bash
curl -x get http://localhost:8000/auth/me \
  -h "authorization: bearer <token>"
```

## archivos

- `utils.py` - funciones de hash y jwt
- `dependencies.py` - dependencias fastapi para validacion
- `schemas.py` - modelos pydantic
- `routes.py` - endpoints de autenticacion
- `hash_password.py` - script para generar hashes

## documentacion completa

ver `auth_documentation.md` en la raiz del proyecto para documentacion completa.
