let util_controller = require('./util_controller');
let index_catalogo = require('../model/elasticsearch/index_catalogos');

exports.obtener_catalogo = function (req, res, next) {
    index_catalogo.obtener_catalogo(req, res, next, util_controller.response_json);
};