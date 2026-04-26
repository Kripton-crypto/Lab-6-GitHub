-- ==========================================================
-- SCRIPT DE TRABAJO: LABORATORIO HASHY EL GOLOSO (MySQL)
-- TEC - Arquitectura de Datos
-- ==========================================================

-- 1. BITÁCORA DE OPERACIONES (Misión de la Llave 6)
-- Registra el rastro de cada transformación del pipeline.
CREATE TABLE logs_hashy (
    id SERIAL PRIMARY KEY,
    nombre_funcion VARCHAR(255),
    fecha_ejecucion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    mensaje_accion TEXT,
    usuario_db VARCHAR(100) DEFAULT (CURRENT_USER())
);

-- 2. MERCADO NEGRO DE TORTUGA (Misión de la Llave 3)
-- Sirve para realizar las subconsultas de comparación de precios.
CREATE TABLE mercado_negro (
    id SERIAL PRIMARY KEY,
    categoria VARCHAR(100) UNIQUE, 
    precio_referencia DECIMAL(10,2),
    ultima_actualizacion DATE
);

-- 3. INVENTARIO DE GOLOSINAS (La tabla principal)
-- Contiene los datos "sucios" que deben ser procesados por las 7 llaves.
CREATE TABLE inventario_pirata (
    id INT PRIMARY KEY,               -- Usado para la Llave 1 (Primalidad)
    nombre_sucio VARCHAR(255),        -- Usado para la Llave 4 (Sanitización)
    categoria VARCHAR(100),           -- Relación con Mercado Negro
    precio_finca DECIMAL(10,2),       -- Usado para la Llave 3 (Tasación)
    prioridad_logica INT,             -- Metadata adicional
    fecha_ingreso DATE,               -- Usado para la Llave 2 (Reloj de Arena)
    meses_validez INT,                -- Usado para la Llave 2 (Reloj de Arena)
    FOREIGN KEY (categoria) REFERENCES mercado_negro(categoria)
);

-- ==========================================================
-- DATOS SEMILLA
-- ==========================================================

-- Llenado del Mercado Negro
INSERT INTO mercado_negro (categoria, precio_referencia, ultima_actualizacion) VALUES 
('Caramelos', 15.00, '2026-01-01'),
('Chocolates', 45.00, '2026-01-01'),
('Gomitas', 20.00, '2026-01-01');

-- Llenado del Inventario
-- Incluimos la 'Gomita Mágica' (ID 7) para que haya variedad en el resultado.
INSERT INTO inventario_pirata (id, nombre_sucio, categoria, precio_finca, prioridad_logica, fecha_ingreso, meses_validez) VALUES 
(1, '  cArr-Amelo_Menta  ', 'Caramelos', 12.00, 2, '2026-02-15', 6),   
(2, 'CHoco-late...Amargo', 'Chocolates', 55.00, 3, '2025-10-01', 3),     
(3, ' gomita-O_O-fresa ', 'Gomitas', 18.00, 4, '2026-03-01', 12),         
(4, '---TRUFA_Oscura---', 'Chocolates', 40.00, 5, '2026-01-10', 5),       
(5, 'Caramelo_Salado!!', 'Caramelos', 18.00, 7, '2025-12-01', 2),         
(6, 'Gomita_Osa', 'Gomitas', 25.00, 11, '2026-04-10', 8),                  
(7, '  !!Gomita_Mágica??  ', 'Gomitas', 22.00, 13, '2026-04-01', 10);  

-- ==========================================================
-- RESULTADO FINAL ESPERADO (VERIFICACIÓN)
-- ==========================================================


-- -------------------------------------------------------------------------
-- Nomeclatura del codigo
-- 
-- Parámetros de entrada: usar prefijo p_
-- Ejemplos: p_id, p_fecha, p_meses, p_cat, p_prec, p_nombre, p_factor, p_texto
--
-- Variables internas/locales: usar prefijo v_
-- Ejemplos: v_resultado, v_fecha_actual, v_fecha_vencimiento
--
-- Regla de nulidad:
-- Si una entrada obligatoria viene NULL, la función devuelve un valor seguro.
-- En validaciones:
--   - fn_cernidor(NULL) devuelve FALSE.
--   - fn_reloj_arena(NULL, NULL) devuelve 'Expirado'.
--
-- Contrato type-safe:
--   fn_cernidor(p_id INT) RETURNS BOOLEAN
--   fn_reloj_arena(p_fecha DATE, p_meses INT) RETURNS VARCHAR(10)
-- ---------------------------------------------------------------------------



-- Los únicos IDs que deben generar un Hash al final son el 3 y el 7.
-- La consulta final debe devolver: hash(ID 3) # hash(ID 7)

-- Funciones de integrante A: Validaciones

Delimiter $$
DROP FUNCTION IF EXISTS fn_cernidor$$

CREATE FUNCTION fn_cernidor(p_id INT)
RETURNS BOOLEAN
DETERMINISTIC
NO SQL
BEGIN
 -- Llave 1: fn_cernidor
    -- Variables:
    --   v_id_validado: copia interna del ID recibido.
    --   v_es_primo: bandera booleana que indica si el número es primo.
    --   v_divisor: contador usado para probar divisores.
    --   v_limite_revision: raíz cuadrada del ID, usada como límite del ciclo.
    --   v_resultado: salida final de la función.


    DECLARE v_id_validado INT DEFAULT 0;
    DECLARE v_es_primo BOOLEAN DEFAULT TRUE;
    DECLARE v_divisor INT DEFAULT 2;
    DECLARE v_limite_revision INT DEFAULT 0;
    DECLARE v_resultado BOOLEAN DEFAULT FALSE;
    
    IF p_id IS NULL THEN
        SET v_resultado = FALSE;
    ELSE
        SET v_id_validado = p_id;

        IF v_id_validado < 2 THEN
            SET v_es_primo = FALSE;
        ELSE
            SET v_limite_revision = FLOOR(SQRT(v_id_validado));

            WHILE v_divisor <= v_limite_revision AND v_es_primo = TRUE DO

                IF MOD(v_id_validado, v_divisor) = 0 THEN
                    SET v_es_primo = FALSE;
                END IF;

                SET v_divisor = v_divisor + 1;

            END WHILE;
        END IF;

        SET v_resultado = v_es_primo;
    END IF;

    RETURN v_resultado;
END$$

DROP FUNCTION IF EXISTS fn_reloj_arena$$

CREATE FUNCTION fn_reloj_arena(p_fecha DATE, p_meses INT)
RETURNS VARCHAR(10)
NOT DETERMINISTIC
NO SQL
BEGIN
    -- Llave 2: fn_reloj_arena
    --   v_fecha_ingreso: copia interna de la fecha recibida.
    --   v_meses_validez: copia interna de los meses recibidos.
    --   v_fecha_actual: fecha actual del servidor.
    --   v_fecha_vencimiento: fecha calculada de vencimiento.
    --   v_estado_resultado: salida final, puede ser 'Fresco' o 'Expirado'.
    DECLARE v_fecha_ingreso DATE;
    DECLARE v_meses_validez INT DEFAULT 0;
    DECLARE v_fecha_actual DATE;
    DECLARE v_fecha_vencimiento DATE;
    DECLARE v_estado_resultado VARCHAR(10) DEFAULT 'Expirado';

    IF p_fecha IS NULL OR p_meses IS NULL THEN
        SET v_estado_resultado = 'Expirado';
    ELSE
        SET v_fecha_ingreso = p_fecha;
        SET v_meses_validez = p_meses;
        SET v_fecha_actual = CURDATE();

        IF v_meses_validez < 0 THEN
            SET v_estado_resultado = 'Expirado';
        ELSE
            SET v_fecha_vencimiento = DATE_ADD(v_fecha_ingreso, INTERVAL v_meses_validez MONTH);

            IF v_fecha_vencimiento > v_fecha_actual THEN
                SET v_estado_resultado = 'Fresco';
            ELSE
                SET v_estado_resultado = 'Expirado';
            END IF;
        END IF;
    END IF;

    RETURN v_estado_resultado;
END$$

DELIMITER ;

SELECT 
    id,
    fn_cernidor(id) AS es_primo
FROM inventario_pirata
ORDER BY id;

-- Funciones de integrante B: Mercado y Limpieza

DELIMITER $$

DROP FUNCTION IF EXISTS fn_purificador$$

CREATE FUNCTION fn_purificador(p_nombre TEXT)
RETURNS TEXT
DETERMINISTIC
NO SQL
BEGIN
    -- Llave 4: fn_purificador
    --   v_nombre_original: copia del texto recibido.
    --   v_nombre_limpio: texto sin caracteres especiales.
    --   v_resultado: salida final de la función.

    DECLARE v_nombre_original TEXT;
    DECLARE v_nombre_limpio TEXT;
    DECLARE v_resultado TEXT DEFAULT '';

    IF p_nombre IS NULL THEN
        SET v_resultado = '';
    ELSE
        SET v_nombre_original = p_nombre;

        -- Eliminar caracteres no alfabéticos
        SET v_nombre_limpio = REGEXP_REPLACE(v_nombre_original, '[^a-zA-ZáéíóúÁÉÍÓÚñÑ]', '');

        -- Quitar espacios sobrantes
        SET v_resultado = TRIM(v_nombre_limpio);
    END IF;

    RETURN v_resultado;
END$$

DELIMITER ;


DELIMITER $$

DROP FUNCTION IF EXISTS fn_espia_tortuga$$

CREATE FUNCTION fn_espia_tortuga(p_cat VARCHAR(100), p_prec DECIMAL(10,2))
RETURNS DECIMAL(3,2)
DETERMINISTIC
READS SQL DATA
BEGIN
    -- Llave 3: fn_espia_tortuga
    --   v_categoria: copia de la categoría recibida.
    --   v_precio_finca: copia del precio recibido.
    --   v_precio_mercado: precio de referencia desde la tabla mercado_negro.
    --   v_resultado: salida final (1.2 o 0.8).

    DECLARE v_categoria VARCHAR(100);
    DECLARE v_precio_finca DECIMAL(10,2);
    DECLARE v_precio_mercado DECIMAL(10,2);
    DECLARE v_resultado DECIMAL(3,2) DEFAULT 0.8;

    IF p_cat IS NULL OR p_prec IS NULL THEN
        SET v_resultado = 0.8;
    ELSE
        SET v_categoria = p_cat;
        SET v_precio_finca = p_prec;

        -- Obtener precio de referencia del mercado
        SELECT precio_referencia
        INTO v_precio_mercado
        FROM mercado_negro
        WHERE categoria = v_categoria
        LIMIT 1;

        IF v_precio_mercado IS NULL THEN
            SET v_resultado = 0.8;
        ELSE
            IF v_precio_finca > v_precio_mercado THEN
                SET v_resultado = 1.2;
            ELSE
                SET v_resultado = 0.8;
            END IF;
        END IF;
    END IF;

    RETURN v_resultado;
END$$

DELIMITER ;

-- =====================================
-- PRUEBAS DE FUNCIONES
-- =====================================

SELECT id, fn_cernidor(id) FROM inventario_pirata;

SELECT id, fn_reloj_arena(fecha_ingreso, meses_validez) 
FROM inventario_pirata;

SELECT nombre_sucio, fn_purificador(nombre_sucio) 
FROM inventario_pirata;

SELECT id, fn_espia_tortuga(categoria, precio_finca) 
FROM inventario_pirata;

SELECT fn_escultor('gomitaofresa', 0.8);
SELECT fn_escultor('gomitamagica', 1.2);

SELECT fn_notario('gomitaofresa_baja_prioridad');
SELECT * FROM logs_hashy;

SELECT fn_gran_sello('gomitaofresa_baja_prioridad');

--Consulta maestra 
SELECT 
    GROUP_CONCAT(
        fn_gran_sello(
            fn_notario(
                fn_escultor(
                    fn_purificador(nombre_sucio),
                    fn_espia_tortuga(categoria, precio_finca)
                )
            )
        )
        ORDER BY id ASC
        SEPARATOR ' # '
    ) AS resultado_final_del_trio
FROM inventario_pirata
WHERE 
    fn_cernidor(id) = TRUE
    AND fn_reloj_arena(fecha_ingreso, meses_validez) = 'Fresco';



SELECT * 
FROM logs_hashy
ORDER BY id;
