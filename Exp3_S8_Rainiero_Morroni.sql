/* =============================================================================
   ACTIVIDAD SUMATIVA 3 - HOTEL "LA ÚLTIMA OPORTUNIDAD"
   ============================================================================= */
SET SERVEROUTPUT ON;

/* =============================================================================
   CASO 1 - IMPLEMENTACION DE TRIGGERS
   ============================================================================= */
-- Trigger a nivel de fila que mantiene sincronizada la tabla TOTAL_CONSUMOS
-- ante eventos DML en la tabla CONSUMO.
CREATE OR REPLACE TRIGGER trg_actualiza_total_consumos
AFTER INSERT OR UPDATE OR DELETE ON consumo
FOR EACH ROW
DECLARE
    v_existe NUMBER; -- Variable para comprobar si el huesped tiene registros
BEGIN    
    -- Si el evento es una Insercion (Nuevo)
    IF INSERTING THEN
        -- Verificamos si el huesped ya existe en la tabla de total_consumos
        SELECT COUNT(*) INTO v_existe 
          FROM total_consumos 
         WHERE id_huesped = :NEW.id_huesped;
        
        -- Si existe, sumamos el nuevo monto al monto existente
        IF v_existe > 0 THEN
            UPDATE total_consumos 
               SET monto_consumos = NVL(monto_consumos, 0) + :NEW.monto
             WHERE id_huesped = :NEW.id_huesped;
        ELSE
            -- Si no existe, se crea el registro inicial para el huesped
            INSERT INTO total_consumos (id_huesped, monto_consumos) 
            VALUES (:NEW.id_huesped, :NEW.monto);
        END IF;
            
    -- Si el evento es una Actualizacion (Modificqar monto)
    ELSIF UPDATING THEN
        -- Restamos el valor antiguo (:OLD) y sumamos el valor nuevo (:NEW)
        UPDATE total_consumos 
           SET monto_consumos = NVL(monto_consumos, 0) - :OLD.monto + :NEW.monto 
         WHERE id_huesped = :NEW.id_huesped;
         
    -- Si el evento es una Eliminacion (Borrar consumo)
    ELSIF DELETING THEN
        -- Restar el valor eliminado (:OLD) del total del huesped
        UPDATE total_consumos 
           SET monto_consumos = NVL(monto_consumos, 0) - :OLD.monto 
         WHERE id_huesped = :OLD.id_huesped;
    END IF;
END;
/
-- ================================================
-- Bloque Anonimo para pruebas de Trigger
-- ================================================
DECLARE
    v_nuevo_id NUMBER;
BEGIN
    /* 
    Durante las pruebas di cuenta de que en cada ejecución se acumulaban $150
    al cliente 340006, por lo que al hacer este DELETE, el trigger restará 
    automáticamente los $150 insertados en la ejecucion previa.
    */
    DELETE FROM consumo WHERE id_reserva = 1587 AND id_huesped = 340006 AND monto = 150;

    -- Se obtiene un ID dinamico para evitar error ORA-00001 (Llave duplicada)
    SELECT NVL(MAX(id_consumo), 0) + 1 INTO v_nuevo_id FROM consumo;

    -- a) Inserta un nuevo consumo al cliente 340006, reserva 1587 y monto $150
    INSERT INTO consumo (id_consumo, id_reserva, id_huesped, monto) 
    VALUES (v_nuevo_id, 1587, 340006, 150);
    
    -- b) Elimina el registro del consumo ID 11473
    DELETE FROM consumo WHERE id_consumo = 11473;
    
    -- c) Actualiza el consumo ID 10688 a $95
    UPDATE consumo SET monto = 95 WHERE id_consumo = 10688;
    
    COMMIT; -- Confirmacion de la transaccion de prueba
    DBMS_OUTPUT.PUT_LINE('Pruebas del Trigger realizadas correctamente.');
END;
/


/* =============================================================================
   CASO 2: IMPLEMENTACION DE PACKAGE Y FUNCIONES
   ============================================================================= */

-- 1. Especificacion del Package
CREATE OR REPLACE PACKAGE pkg_hotel IS
    -- Variable publica para almacenar el valor de los tours del huesped
    v_monto_tours NUMBER;
    
    -- Funcion publica para calcular el total en USD de los tours tomados
    FUNCTION fn_monto_tours(p_id_huesped NUMBER) RETURN NUMBER;
END pkg_hotel;
/

-- 2. Cuerpo del Package
CREATE OR REPLACE PACKAGE BODY pkg_hotel IS
    -- Funcion que calcula el total de tours
    FUNCTION fn_monto_tours(p_id_huesped NUMBER) RETURN NUMBER IS
        v_total_usd NUMBER := 0;
    BEGIN
        -- Calculo de valor del tour por el numero de personas
        SELECT NVL(SUM(t.valor_tour * ht.num_personas), 0)
          INTO v_total_usd
          FROM huesped_tour ht
          JOIN tour t ON ht.id_tour = t.id_tour
         WHERE ht.id_huesped = p_id_huesped;
         
        RETURN v_total_usd;
    EXCEPTION
        -- Retorna 0
        WHEN OTHERS THEN
            RETURN 0;
    END fn_monto_tours;
END pkg_hotel;
/

-- 3. Funcion Almacenada: Obtener Agencia del huesped
CREATE OR REPLACE FUNCTION fn_obtener_agencia(p_id_huesped NUMBER) RETURN VARCHAR2 IS
    v_nom_agencia VARCHAR2(40);
    v_err_msg     VARCHAR2(300);
BEGIN
    -- Obtener el nombre de la agencia asociada al huesped
    SELECT a.nom_agencia
      INTO v_nom_agencia
      FROM huesped h
      JOIN agencia a ON h.id_agencia = a.id_agencia
     WHERE h.id_huesped = p_id_huesped;
     
    RETURN v_nom_agencia;
EXCEPTION
    -- Si el huesped no tiene agencia, se captura error y se registra en reg_errores
    WHEN NO_DATA_FOUND THEN
        v_err_msg := SQLERRM;
        INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
        VALUES (sq_error.NEXTVAL, 'Error al recuperar la agencia del huesped con id ' || p_id_huesped, v_err_msg);
        RETURN 'NO REGISTRA AGENCIA';
END fn_obtener_agencia;
/

-- 4. Funcion Almacenada: Obtener Consumos
CREATE OR REPLACE FUNCTION fn_obtener_consumos(p_id_huesped NUMBER) RETURN NUMBER IS
    v_monto_consumos NUMBER;
    v_err_msg        VARCHAR2(300);
BEGIN
    -- Se consulta la tabla TOTAL_CONSUMOS
    SELECT monto_consumos
      INTO v_monto_consumos
      FROM total_consumos
     WHERE id_huesped = p_id_huesped;
     
    RETURN v_monto_consumos;
EXCEPTION
    -- Si no registra consumos, se captura el error y se registra en reg_errores
    WHEN NO_DATA_FOUND THEN
        v_err_msg := SQLERRM;
        INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
        VALUES (sq_error.NEXTVAL, 'Error al recuperar los consumos del cliente con id ' || p_id_huesped, v_err_msg);
        RETURN 0;
END fn_obtener_consumos;
/

/* =============================================================================
   ALMACENAMIENTO Y REGLAS DE NEGOCIO
   ============================================================================= */

CREATE OR REPLACE PROCEDURE sp_calcular_pagos(p_fecha_proceso VARCHAR2, p_valor_dolar NUMBER) IS
    -- Conversion del parametro VARCHAR2 a DATE para realizar calculos de fechas
    v_fecha_corte DATE := TO_DATE(p_fecha_proceso, 'DD/MM/YYYY');
    
    -- Cursor Explicito que obtiene los huespedes que terminan su estadia en la fecha solicitada
    CURSOR c_huespedes IS
        SELECT r.id_reserva, h.id_huesped,
               SUBSTR(h.appat_huesped || ' ' || h.nom_huesped, 1, 60) AS nombre_completo,
               r.estadia
          FROM reserva r
          JOIN huesped h ON r.id_huesped = h.id_huesped
         WHERE (r.ingreso + r.estadia) = v_fecha_corte;
         
    -- Variables para las logicas de negocio
    v_agencia           VARCHAR2(40);
    v_valor_persona_clp NUMBER := 35000; -- Costo fijo de $35.000 por persona
    v_pct_desc_cons     NUMBER;
    
    -- Variables para calculos en USD
    v_alojamiento_usd   NUMBER;
    v_consumos_usd      NUMBER;
    
    -- Variables para calculos finales en Pesos (CLP) (redondeados)
    v_alojamiento_clp   NUMBER;
    v_consumos_clp      NUMBER;
    v_tours_clp         NUMBER;
    v_subtotal_clp      NUMBER;
    v_desc_cons_clp     NUMBER;
    v_desc_agencia_clp  NUMBER;
    v_total_clp         NUMBER;
    
    v_err_msg           VARCHAR2(300);

BEGIN
    -- Limpieza de tablas de resultados y errores en tiempo de ejecucion
    EXECUTE IMMEDIATE 'TRUNCATE TABLE detalle_diario_huespedes';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE reg_errores';

    -- Inicio del procesamiento fila por fila
    FOR r_huesped IN c_huespedes LOOP
        
        -- Obtencion de Agencia y Consumos mediante las funciones almacenadas
        v_agencia      := fn_obtener_agencia(r_huesped.id_huesped);
        v_consumos_usd := fn_obtener_consumos(r_huesped.id_huesped);
        
        -- Obtencion del valor de tours usando el Package
        pkg_hotel.v_monto_tours := pkg_hotel.fn_monto_tours(r_huesped.id_huesped);
        
        -- Calculo del Alojamiento (valor de la habitación + minibar) * estadia
        SELECT NVL(SUM((hab.valor_habitacion + hab.valor_minibar) * r_huesped.estadia), 0)
          INTO v_alojamiento_usd
          FROM detalle_reserva dr
          JOIN habitacion hab ON dr.id_habitacion = hab.id_habitacion
         WHERE dr.id_reserva = r_huesped.id_reserva;
         
        -- Conversion de USD a CLP
        v_alojamiento_clp := ROUND(v_alojamiento_usd * p_valor_dolar);
        v_consumos_clp    := ROUND(v_consumos_usd * p_valor_dolar);
        v_tours_clp       := ROUND(pkg_hotel.v_monto_tours * p_valor_dolar);
        
        -- Calculo del Subtotal: Alojamiento + Consumos + Valor Fijo por Persona
        v_subtotal_clp := v_alojamiento_clp + v_consumos_clp + v_valor_persona_clp;
        
        -- Obtener porcentaje de descuento por consumos y tramo
        BEGIN
            SELECT pct INTO v_pct_desc_cons
              FROM tramos_consumos
             WHERE v_consumos_usd BETWEEN vmin_tramo AND vmax_tramo;
        EXCEPTION
            -- Si no existe informacion, no aplica descuento
            WHEN NO_DATA_FOUND THEN
                v_pct_desc_cons := 0;
        END;
        
        -- Calculo del descuento por consumo
        v_desc_cons_clp := ROUND((v_consumos_usd * v_pct_desc_cons) * p_valor_dolar);
        
        -- Descuento especial del 12% por agencia VIAJES ALBERTI
        IF v_agencia = 'VIAJES ALBERTI' THEN
            v_desc_agencia_clp := ROUND(v_subtotal_clp * 0.12); 
        ELSE
            v_desc_agencia_clp := 0;
        END IF;
        
        -- Calculo del Total a Pagar
        v_total_clp := (v_subtotal_clp + v_tours_clp) - v_desc_cons_clp - v_desc_agencia_clp;
        
        -- Insercion en la tabla resumen
        INSERT INTO detalle_diario_huespedes 
            (id_huesped, nombre, agencia, alojamiento, consumos, tours, 
             subtotal_pago, descuento_consumos, descuentos_agencia, total)
        VALUES 
            (r_huesped.id_huesped, r_huesped.nombre_completo, v_agencia, v_alojamiento_clp, 
             v_consumos_clp, v_tours_clp, v_subtotal_clp, v_desc_cons_clp, v_desc_agencia_clp, v_total_clp);
             
    END LOOP;
    
    COMMIT; -- Confirmar la transaccion completa
    
EXCEPTION
    -- Manejo global de excepciones para proteger la integridad del proceso
    WHEN OTHERS THEN
        ROLLBACK;
        v_err_msg := SUBSTR(SQLERRM, 1, 200); 
        INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
        VALUES (sq_error.NEXTVAL, 'SP_CALCULAR_PAGOS', 'Error Critico: ' || v_err_msg);
        COMMIT;
END sp_calcular_pagos;
/



-- Bloque Anonimo de Ejecucion Final para la fecha y valor de USD solicitado
BEGIN    
    sp_calcular_pagos('18/08/2021', 915);
    DBMS_OUTPUT.PUT_LINE('CASO 2: Procedimiento Principal ejecutado exitosamente.');
END;
/


/*
-- ======================================================
-- Opcion de ejecucion principal parametrica por consola
-- ======================================================
DECLARE
    v_fecha_input VARCHAR2(10) := '&Ingrese_Fecha_DD_MM_YYYY)';
    v_dolar_input NUMBER       := &Ingrese_Valor_Dolar;
BEGIN    
    sp_calcular_pagos(v_fecha_input, v_dolar_input);
    DBMS_OUTPUT.PUT_LINE('Procedimiento Principal ejecutado exitosamente para la fecha ' || v_fecha_input);
END;
/
*/


/* =============================================================================
   CONSULTAS DE COMPROBACION FINAL
   ============================================================================= */
-- === RESULTADOS TABLA TOTAL_CONSUMOS POR RESERVA (CASO 1) ===;
SELECT * FROM total_consumos WHERE id_huesped IN (340003, 340004, 340006, 340008, 340009) ORDER BY id_huesped;

-- === RESULTADOS TABLA DETALLE_DIARIO_HUESPEDES (CASO 2) ===;
SELECT * FROM detalle_diario_huespedes ORDER BY id_huesped;

-- === RESULTADOS TABLA REG_ERRORES (CASO 2) ===;
SELECT * FROM reg_errores ORDER BY id_error;

-- Fin actividad Sumativa 3 - Programacion Base de Datos PRY2206
-- Rainiero Morroni - DUOC 2026