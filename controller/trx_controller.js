var util_controller = require('./util_controller');
var db_trx = require('../model/mongoDB/db_trx');

exports.obtener_trx_n_periodo = function (req, res, next) {
    db_trx.obtener_trx_n_periodo(req, res, next,util_controller.response_json);
};

exports.obtener_comercios=function(req, res, next){
    db_trx.obtener_comercios(req, res, next, util_controller.response_json);
}