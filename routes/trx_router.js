let express = require('express');
let router = express.Router();
let trx_controller = require('../controller/trx_controller');
//let comercios_controller = require('../controller/comercios_controller');

//Añadir periodo Anual, mensual, diario. 
//Añadir suma de las transacciones por producto group producto sum 
//

router.get('/obtener_trx', trx_controller.obtener_trx);
router.get('/obtener_comercios', trx_controller.obtener_comercios);
router.get('/pagos', trx_controller.obtener_pagos);
router.get('/obtener_trx_n_periodo', trx_controller.obtener_trx_n_periodo);
router.get('/obtener_promedio_trx_n_periodo', trx_controller.obtener_promedio_trx_n_periodo);
router.get('/obtener_suma_trx_n_periodo', trx_controller.obtener_suma_trx_n_periodo);
router.get('/obtener_promedio_trx', trx_controller.obtener_promedio_trx);
router.get('/obtener_n_trx_n_periodo', trx_controller.obtener_n_trx_n_periodo);
router.get('/obtener_n_trx_n_periodos', trx_controller.obtener_n_trx_n_periodos);
router.get('/obtener_promedio_pago_n_periodo', trx_controller.obtener_promedio_pago_n_periodo);
router.get('/obtener_n_pago_n_periodo', trx_controller.obtener_n_pago_n_periodo);
router.get('/obtener_comercios_por_locacion', trx_controller.obtener_comercios_por_locacion);
router.get('/obtener_comercios_por_fecha_creacion', trx_controller.obtener_comercios_por_fecha_creacion);
router.get('/obtener_rfm', trx_controller.obtener_rfm);
router.get('/obtener_grupo_rfm', trx_controller.obtener_grupo_rfm);
router.get('/obtener_num_comercios_grupo_rfm', trx_controller.obtener_num_comercios_grupo_rfm);
router.get('/obtener_promedio_trx_n_periodo_por_comercio', trx_controller.obtener_promedio_trx_n_periodo_por_comercio);
router.get('/obtener_informacion_por_locacion', trx_controller.obtener_informacion_por_locacion);
router.get('/obtener_total_monetario_trx_n_periodo_x_lugar', trx_controller.obtener_total_monetario_trx_n_periodo_x_lugar);
router.get('/obtener_total_trx_n_periodo_x_lugar', trx_controller.obtener_total_trx_n_periodo_x_lugar);
router.get('/obtener_suma_trx_n_periodo_x_lugar', trx_controller.obtener_suma_trx_n_periodo_x_lugar);
router.get('/obtener_cantidad_trx_x_periodo_por_locacion', trx_controller.obtener_cantidad_trx_x_periodo_por_locacion);
router.get('/obtener_grupo_rfm_con_actividad', trx_controller.obtener_grupo_rfm_con_actividad);
router.get('/obtener_grupo_rfm_por_locacion', trx_controller.obtener_grupo_rfm_por_locacion);
router.get('/obtener_grupo_rfm_por_locacion_sin_actividad', trx_controller.obtener_grupo_rfm_por_locacion_sin_actividad);
router.get('/obtener_grupo_rfm_por_nivel_geografico', trx_controller.obtener_grupo_rfm_por_nivel_geografico);
router.get('/obtener_ejecutivos', trx_controller.obtener_ejecutivos);
router.get('/obtener_visitas_ejecutivos', trx_controller.obtener_visitas_ejecutivos);


module.exports = router;