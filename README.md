# Instrucciones de ejecución

## Crear contenedores

1. Crear imagen principal
    ```
    sh build_image.sh
    ```
    ---
    **&#9432; Nota:**  
    Asegúrate que Docker esté corriendo

    ---

2. Correr imagen con Postgresql de fondo
    ```
    sh run_db_image.sh
    ```

3. Corre el contenedor principal:
    ```
    sh run_image.sh
    ```


## ETL (Extract, Transform, Load)

1. Cargar y limpiar archivo excel de base de datos SIPRI sobre gasto militar. 
Única sección que usa pandas.

    ```
    python src/etl/load_excels.py
    ```

2. ETL principal: Corre limpieza, transformación, eliminación de nulos, estandarización 
de nombres, etc. y escribe a base de datos.

    a. Ejecutar programa y escribir a base de datos
    ```
    sh run_etl.sh
    ```

    b. Verificar base de datos
    ```
    sh run_verify_db.sh
    ```

3. Programa de Aprendizaje Automático: