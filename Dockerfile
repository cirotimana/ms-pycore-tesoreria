FROM python:3.12-slim-bookworm

ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

# Instala herramientas necesarias y genera locales
RUN apt-get update && apt-get install -y --no-install-recommends \
    gnupg2 curl apt-transport-https software-properties-common \
    gcc libpq-dev unixodbc unixodbc-dev locales \
    && locale-gen en_US.UTF-8

# Agrega la nueva clave GPG de Microsoft correctamente (actualizada)
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg

# Agrega el repositorio de Microsoft para Debian 12 (bookworm)
RUN echo "deb [arch=amd64 signed-by=/etc/apt/trusted.gpg.d/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
    > /etc/apt/sources.list.d/mssql-release.list

# Instala el driver ODBC 18
RUN apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Copia el proyecto y instala dependencias Python
WORKDIR /app
COPY . /app

# Instala dependencias de Playwright manualmente
RUN apt-get update && apt-get install -y \
    libnspr4 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libatspi2.0-0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libxcb1 \
    libxkbcommon0 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*


RUN pip install --upgrade pip \
    && pip install -r requirements.txt

RUN playwright install --with-deps chromium
# RUN playwright install chrome

EXPOSE 3023

# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3023"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3023", "--workers", "1"]



