# Utiliza una imagen base de Python
FROM python:3.9-slim

# Establece el directorio de trabajo en el contenedor
WORKDIR /app

# Copia los archivos requeridos al contenedor
COPY . /app

# Instala las dependencias necesarias
RUN pip install --no-cache-dir -r requirements.txt

# Expone el puerto si es necesario (opcional, si la aplicación escucha en un puerto)
# EXPOSE 8000

# Comando para ejecutar la aplicación
CMD ["python", "entrega1-variacion-dolar.py"]
