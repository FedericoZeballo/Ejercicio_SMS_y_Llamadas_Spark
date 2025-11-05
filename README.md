# README

Ejercicio de facturación de SMS y llamadas para **GRANDATA**.

Este documento explica paso a paso cómo se ha resuelto los ejercicios solicitados como parte del proceso de entrevistas para el rol de Data Engineer.

El proyecto propone el procesamiento de eventos telefónicos entre usuarios por medio de SMS y llamados.  
Para su resolución se utilizó Apache Spark 2.3 dentro de un entorno Docker.

Se utilizaron notebooks en Jupyter, uno para funciones reutilizables y otro como ejecutor del flujo completo.

## Estructura del proyecto

El proyecto está organizado en las siguientes carpetas y archivos:
```text
├── work/
│   ├── SMS_Facturacion_Funciones.py            --> Notebook con funciones reutilizables.
│   ├── SMS_Facturacion_Ejecutor.ipynb          --> Notebook que ejecuta el flujo completo.
│   └── SMS_Facturacion_Exploracion.ipynb       --> Notebook que contiene una breve exploración y validaciones.
├── data_in/
│   ├── events.csv.gz                           --> Dataset principal con eventos de comunicación.
│   └── free_sms_destinations.csv.gz            --> Lista de destinos gratuitos para SMS.
├── data_out/
│   ├── top_usuarios_sms/                       --> Dataset en formato Parquet con los 100 usuarios top.
│   └── histograma_llamadas.png                 --> Imagen del histograma de llamadas por hora.
├── README.md                                   --> Documentación del proyecto.
├── docker-compose.yml                          --> Configuración del entorno Docker.
└── Respuestas_GranData                         --> Contiene las respuestas al cuestionario dado.
```

## Paso a paso del proyecto

1. **Carga de datos**  
   Se leen los archivos comprimidos `events.csv.gz` y `free_sms_destinations.csv.gz` usando Spark. Se descartan los registros con IDs nulos.

2. **Cálculo de facturación por SMS**  
   Se calcula el monto facturado por cada SMS enviado, considerando los valores:
   - 0 si el destino es gratuito.
   - 1.5 si la región está entre 1 y 5.
   - 2.0 si la región está entre 6 y 9.

3. **Top 100 usuarios por facturación**  
   Se agrupan los montos por usuario origen (`id_source`), se ordenan de mayor a menor y se seleccionan los 100 con mayor facturación. Además, se genera un hash MD5 del ID para proteger la identidad del usuario.

4. **Exportación de resultados**  
   El dataset de los 100 usuarios se guarda en formato Parquet con compresión gzip en la carpeta `data_out/top_usuarios_sms`.

5. **Histograma de llamadas por hora**  
   Se agrupan las llamadas por hora del día y se genera un gráfico de barras que se guarda como imagen PNG en `data_out/histograma_llamadas.png`.

## Instrucciones de ejecución

1. Clonar el repositorio
   Primero, cloná el repositorio de GitHub en tu máquina local:
   
```bash
git clone https://github.com/FedericoZeballo/Ejercicio_SMS_y_Llamadas_Spark.git
cd Ejercicio_SMS_y_Llamadas_Spark
   ```

2. Levantar el entorno Docker con Spark ejecutando en terminal:
 ```bash  
    docker-compose up -d
 ```
3. Ejecutar el notebook ejecutor:

      SMS_Facturacion_Ejecutor.ipynb
