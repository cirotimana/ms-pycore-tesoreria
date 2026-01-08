from app.auth.utils import hash_password


def main():
    print("=== generador de hash de contraseña (sha256) ===\n")

    while True:
        password = input("ingrese la contraseña a hashear (o 'exit' para salir): ")

        if password.lower() == "exit":
            print("saliendo...")
            break

        if not password:
            print("error: la contraseña no puede estar vacia\n")
            continue

        hashed = hash_password(password)
        print(f"\ncontraseña original: {password}")
        print(f"hash sha256: {hashed}")
        print(f"\nquery sql de ejemplo:")
        print(f"update users set password = '{hashed}' where username = 'tu_usuario';")
        print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    main()
