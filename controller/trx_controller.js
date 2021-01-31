var util_controller = require('./util_controller');
var db_trx = require('../model/mongoDB/db_trx');
var db_comercio= require('../model/mongoDB/db_comercio');
var db_pago= require('../model/mongoDB/db_pago');
var db_ejecutivo= require('../model/mongoDB/db_ejecutivo');

exports.obtener_trx = function (req, res, next) {
    db_trx.obtener_trx(req, res, next,util_controller.response_json);
};

exports.obtener_trx_n_periodo=function(req, res, next){
    db_trx.obtener_trx_n_periodo(req, res, next, util_controller.response_json);
}

exports.obtener_promedio_trx=function(req, res, next){
    db_trx.obtener_promedio_trx(req, res, next, util_controller.response_json);
}

exports.obtener_promedio_trx_n_periodo=function(req, res, next){
    db_trx.obtener_promedio_trx_n_periodo(req, res, next, util_controller.response_json);
}

exports.obtener_suma_trx_n_periodo=function(req, res, next){
    db_trx.obtener_suma_trx_n_periodo(req, res, next, util_controller.response_json);
}

exports.obtener_n_trx_n_periodo=function(req, res, next){
    db_trx.obtener_n_trx_n_periodo(req, res, next, util_controller.response_json);
}

exports.obtener_n_trx_n_periodos=function(req, res, next){
    db_trx.obtener_n_trx_n_periodos(req, res, next, util_controller.response_json);
}


exports.obtener_promedio_trx_n_periodo_por_comercio=function(req, res, next){
    db_trx.obtener_promedio_trx_n_periodo_por_comercio(req, res, next, util_controller.response_json);
}

exports.obtener_informacion_por_locacion=function(req, res, next){
    db_trx.obtener_informacion_por_locacion(req, res, next, util_controller.response_json);
}

exports.obtener_total_monetario_trx_n_periodo_x_lugar=function(req, res, next){
    db_trx.obtener_total_monetario_trx_n_periodo_x_lugar(req, res, next, util_controller.response_json);
}

exports.obtener_total_trx_n_periodo_x_lugar=function(req, res, next){
    db_trx.obtener_total_trx_n_periodo_x_lugar(req, res, next, util_controller.response_json);
}

exports.obtener_suma_trx_n_periodo_x_lugar=function(req, res, next){
    db_trx.obtener_suma_trx_n_periodo_x_lugar(req, res, next, util_controller.response_json);
}

exports.obtener_cantidad_trx_x_periodo_por_locacion=function(req, res, next){
    db_trx.obtener_cantidad_trx_x_periodo_por_locacion(req, res, next, util_controller.response_json);
}

exports.obtener_valor_total_maximo=function(req, res, next){
    db_trx.obtener_valor_total_maximo(req, res, next, util_controller.response_json);
}

exports.obtener_cantidad_maxima=function(req, res, next){
    db_trx.obtener_cantidad_maxima(req, res, next, util_controller.response_json);
}

exports.obtener_suma_trx_por_comercio=function(req, res, next){
    db_trx.obtener_suma_trx_por_comercio(req, res, next, util_controller.response_json);
}

exports.obtener_suma_trx_n_periodo_x_lugar_before=function(req, res, next){
    db_trx.obtener_suma_trx_n_periodo_x_lugar_before(req, res, next, util_controller.response_json);
}

exports.obtener_pagos=function(req, res, next){
    db_pago.obtener_pagos(req, res, next, util_controller.response_json);
}

exports.obtener_promedio_pago_n_periodo=function(req, res, next){
    db_pago.obtener_promedio_pago_n_periodo(req, res, next, util_controller.response_json);
}

exports.obtener_n_pago_n_periodo=function(req, res, next){
    db_pago.obtener_n_pago_n_periodo(req, res, next, util_controller.response_json);
}


exports.obtener_comercios=function(req, res, next){
    db_comercio.obtener_comercios(req, res, next, util_controller.response_json);
}

exports.obtener_comercios_por_locacion=function(req, res, next){
    db_comercio.obtener_comercios_por_locacion(req, res, next, util_controller.response_json);
}

exports.obtener_comercios_por_fecha_creacion=function(req, res, next){
    db_comercio.obtener_comercios_por_fecha_creacion(req, res, next, util_controller.response_json);
}

exports.obtener_rfm=function(req, res, next){
    db_comercio.obtener_rfm(req, res, next, util_controller.response_json);
}

exports.obtener_grupo_rfm=function(req, res, next){
    db_comercio.obtener_grupo_rfm(req, res, next, util_controller.response_json);
}

exports.obtener_num_comercios_grupo_rfm=function(req, res, next){
    db_comercio.obtener_num_comercios_grupo_rfm(req, res, next, util_controller.response_json);
}

exports.obtener_grupo_rfm_con_actividad=function(req, res, next){
    db_comercio.obtener_grupo_rfm_con_actividad(req, res, next, util_controller.response_json);
}

exports.obtener_grupo_rfm_por_locacion=function(req, res, next){
    db_comercio.obtener_grupo_rfm_por_locacion(req, res, next, util_controller.response_json);
}

exports.obtener_grupo_rfm_por_locacion_sin_actividad=function(req, res, next){
    db_comercio.obtener_grupo_rfm_por_locacion_sin_actividad(req, res, next, util_controller.response_json);
}

exports.obtener_grupo_rfm_por_nivel_geografico=function(req, res, next){
    db_comercio.obtener_grupo_rfm_por_nivel_geografico(req, res, next, util_controller.response_json);
}

exports.obtener_informacion_comercios=function(req, res, next){
    db_comercio.obtener_informacion_comercios(req, res, next, util_controller.response_json);
}

exports.obtener_ejecutivos=function(req, res, next){
    db_ejecutivo.obtener_ejecutivos(req, res, next, util_controller.response_json);
}

exports.obtener_visitas_ejecutivos=function(req, res, next){
    db_ejecutivo.obtener_visitas_ejecutivos(req, res, next, util_controller.response_json);
}









