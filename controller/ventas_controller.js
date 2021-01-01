var util_controller = require('./util_controller');
var index_resumen_ventas_mensuales = require('../model/elasticsearch/index_resumen_ventas_mensuales');

exports.obtener_ventas_n_periodo = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_ventas_n_periodo(req, res, next,util_controller.response_json);
};

exports.obtener_ventas_por_region = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_ventas_por_region(req, res, next,util_controller.response_json);
};
exports.obtener_ventas_por_region_n_anios = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_ventas_por_region_n_anios(req, res, next,util_controller.response_json);
};

exports.obtener_ventas_por_provincia_periodo = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_ventas_por_provincia_periodo(req, res, next,util_controller.response_json);
};

exports.obtener_ventas_por_ciudad_periodo = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_ventas_por_ciudad_periodo(req, res, next,util_controller.response_json);
};

exports.obtener_ventas_por_proveedor_por_plataforma_periodo = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_ventas_por_proveedor_por_plataforma_periodo(req, res, next,util_controller.response_json);
};

exports.obtener_ventas_por_categoria_por_plataforma_periodo = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_ventas_por_categoria_por_plataforma_periodo(req, res, next, util_controller.response_json);
};

exports.obtener_ventas_por_modelo_negocio_periodo = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_ventas_por_modelo_negocio_periodo(req, res, next, util_controller.response_json);
};

exports.obtener_ventas_por_cuadrante = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_ventas_por_cuadrante(req, res, next, util_controller.response_json);
};


exports.obtener_ventas = function (req, res, next) {
    index_resumen_ventas_mensuales.obtener_ventas(req, res, next,util_controller.response_json);
};

exports.obtener_ventas_etiqueta_proveedor = function (req, res, next){
  index_resumen_ventas_mensuales.obtener_ventas_etiqueta_proveedor(req, res, next,util_controller.response_json);
};







