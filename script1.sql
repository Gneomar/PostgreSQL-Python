
DO $$
DECLARE
    fila RECORD;
BEGIN
    -- Iterar sobre las filas a insertar
    FOR fila IN
        SELECT * FROM (VALUES
            (5, 'Martin', 'Martin', 85, 20.45),
            (3, 'Pamela', 'Pamela', 12, 322.41),
            (9, 'Derek', 'Derek', 4, 694.32),
            (17, 'Joel', 'Joel', 70, 380.92),
            (3, 'Adam', 'Adam', 87, 158.77),
            (6, 'Gabriel', 'Gabriel', 31, 707.34),
            (16, 'Ariel', 'Ariel', 9, 268.05),
            (16, 'Jesse', 'Jesse', 80, 860.97),
            (12, 'Brooke', 'Brooke', 87, 72.09),
            (12, 'Brian', 'Brian', 72, 930.27),
            (15, 'Lisa', 'Lisa', 12, 778.4),
            (9, 'Barbara', 'Barbara', 73, 714.86),
            (1, 'Emma', 'Emma', 84, 973.72),
            (18, 'Clinton', 'Clinton', 38, 484.67),
            (1, 'John', 'John', 69, 764.8),
            (19, 'Angela', 'Angela', 27, 934.29),
            (14, 'Nathan', 'Nathan', 84, 909.33),
            (20, 'Joshua', 'Joshua', 62, 153.05),
            (3, 'Russell', 'Russell', 48, 57.09),
            (4, 'Kimberly', 'Kimberly', 62, 370.92),
            (17, 'Tyler', 'Tyler', 7, 206.54),
            (15, 'Randy', 'Randy', 31, 66.94),
            (12, 'Deborah', 'Deborah', 18, 916.05),
            (3, 'Carlos', 'Carlos', 16, 277.24),
            (5, 'Richard', 'Richard', 34, 688.65),
            (12, 'Daniel', 'Daniel', 83, 409.02),
            (4, 'Ann', 'Ann', 51, 560.43),
            (11, 'Lance', 'Lance', 98, 637.67),
            (15, 'Tara', 'Tara', 41, 606.69),
            (2, 'Jose', 'Jose', 94, 438.26),
            (2, 'Daniel', 'Daniel', 64, 160.48),
            (12, 'Natalie', 'Natalie', 94, 365.65),
            (10, 'Willie', 'Willie', 22, 463.47),
            (11, 'Brian', 'Brian', 75, 990.57),
            (20, 'Timothy', 'Timothy', 36, 776.57),
            (9, 'Mark', 'Mark', 7, 763.43),
            (9, 'Alexis', 'Alexis', 19, 739.49),
            (2, 'Kimberly', 'Kimberly', 46, 653.84),
            (18, 'Renee', 'Renee', 60, 447.77),
            (20, 'Brian', 'Brian', 23, 911.47),
            (16, 'Heidi', 'Heidi', 25, 975.19),
            (7, 'Steven', 'Steven', 8, 678.47),
            (12, 'Kyle', 'Kyle', 45, 213.57),
            (12, 'Kristen', 'Kristen', 7, 938.99),
            (13, 'Connor', 'Connor', 87, 993.12),
            (20, 'Thomas', 'Thomas', 6, 645.72),
            (13, 'Grant', 'Grant', 84, 642.71),
            (19, 'James', 'James', 22, 229.53),
            (10, 'Cory', 'Cory', 16, 762.84),
            (2, 'Jeffrey', 'Jeffrey', 67, 293.89)
        ) AS datos(dni, nombre, apellido, cantidad, monto)
    LOOP
        BEGIN
            -- Intentar insertar la fila
            INSERT INTO tabla1 (dni, nombre, apellido, cantidad, monto)
            VALUES (fila.dni, fila.nombre, fila.apellido, fila.cantidad, fila.monto) 
            ON CONFLICT (dni) 
            DO UPDATE SET nombre = EXCLUDED.nombre, apellido = EXCLUDED.apellido, cantidad = EXCLUDED.cantidad, monto = EXCLUDED.monto
        ;

        EXCEPTION
            WHEN unique_violation THEN
                -- Capturar el conflicto y registrar el error con detalles específicos
                INSERT INTO errores_ejemplo_tabla (table_name, error_message, data)
                VALUES (
                    'tabla1', 
                    'Unique violation error: Conflict in columns dni, nombre, apellido, cantidad, monto', 
                    row_to_json(fila)
                );
            WHEN foreign_key_violation THEN
                -- Error de clave foránea
                INSERT INTO errores_ejemplo_tabla (table_name, error_message, data)
                VALUES (
                    'tabla1', 
                    'Foreign key violation error: Invalid foreign key value in row', 
                    row_to_json(fila)
                );
            WHEN check_violation THEN
                -- Error de violación de restricciones de CHECK
                INSERT INTO errores_ejemplo_tabla (table_name, error_message, data)
                VALUES (
                    'tabla1', 
                    'Check constraint violation error: Invalid value in row',
                    row_to_json(fila)
                );
            WHEN others THEN
                -- Registrar error general con detalles
                INSERT INTO errores_ejemplo_tabla (table_name, error_message, data)
                VALUES (
                    'tabla1', 
                    'General error: ' || SQLERRM || ' | Row data: ' || row_to_json(fila)::text, 
                    row_to_json(fila)
                );
        END;
    END LOOP;
END $$;
