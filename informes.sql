DROP TABLE IF EXISTS informes;
CREATE TABLE informes (
    id BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    "ruta 3 nivel" TEXT,
    "Codificacion" TEXT,
    "Carpeta Etapa" TEXT,
    "Codificacion Etapa" TEXT,
    "Carpeta Informe" TEXT,
    "Categoria" TEXT,
    "guiones" TEXT,
    "Sin 0 Izquierda" TEXT,
    "Es Numero" TEXT,
    "Numero Informe" TEXT,
    "Archivo BBDD" TEXT,
    "Estructura Informe" TEXT,
    "Excel Informe" TEXT,
    "Errores" INTEGER
);

-- Crear la funcion para actualizar updated_at
CREATE OR REPLACE FUNCTION update_informes_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Crear el trigger que llama a la funcion antes de actualizar
CREATE TRIGGER update_informes_updated_at_trigger
BEFORE UPDATE ON informes
FOR EACH ROW
EXECUTE FUNCTION update_informes_updated_at();