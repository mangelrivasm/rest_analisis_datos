var util_controller = require('./util_controller');
var index_resumen_ventas_mensuales = require('../model/elasticsearch/index_resumen_ventas_mensuales');

exports.obtener_numero_puntos_venta_mensual = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_numero_puntos_venta_mensual(req, res, next, util_controller.response_json);
};

exports.obtener_numero_puntos_venta_nuevos_mensual = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_numero_puntos_venta_nuevos_mensual(req, res, next, util_controller.response_json);
};

exports.obtener_numero_puntos_venta_anual = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_numero_puntos_venta_anual(req, res, next, util_controller.response_json);
};

exports.obtener_numero_puntos_venta_nuevos_anual = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_numero_puntos_venta_nuevos_anual(req, res, next, util_controller.response_json);
};