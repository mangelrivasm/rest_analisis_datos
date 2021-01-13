let mongo_connection = require('./connection');

exports.obtener_trx_n_periodo = function (req, res, next, res_function) {
    var dbo=mongo_connection.dbo;
    dbo.collection("transaccion_log").find().toArray(function(err, result) {
        if (err) console.log(err);
        res_function(null,result, req, res, next);
    });
};

exports.obtener_promedio_ventas = function(req, res, next, res_function){

}

exports.obtener_comercios = function (req, res, next, res_function) {
    var dbo=mongo_connection.dbo;
    dbo.collection("comercio").find().toArray(function(err, result) {
        if (err) console.log(err);
        res_function(null,result, req, res, next);
      });
};

