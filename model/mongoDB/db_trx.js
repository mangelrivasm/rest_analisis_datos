const { query } = require('express');
let mongo_connection = require('./connection');

function capitalizeWords(word){
  word=word.toLowerCase();
  let finalSentence = word.replace(/(^\w{1})|(\s+\w{1})/g, letter => letter.toUpperCase())
  return finalSentence;
}

exports.obtener_trx = function (req, res, next, res_function) {
    var dbo=mongo_connection.dbo;
    dbo.collection("transaccion_log").find().toArray(function(err, result) {
        if (err) console.log(err);
        res_function(null,result, req, res, next);
    });
};

exports.obtener_trx_n_periodo = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';
  let producto = req.query.producto? req.query.producto:'';
  let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                     "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                     "barrio": new RegExp('^' + capitalizeWords(barrio))};
  dbo.collection("comercio").distinct("codigo_merchant", querycomercio, 
    (function(err, result) {
      if (err) console.log(err);
      let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
      dbo.collection("transaccion_log").find(querytrx).toArray(function(err2, resulttrx) {
        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
      });
    }));
}


exports.obtener_promedio_trx_n_periodo = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let comercio = req.query.comercio? req.query.comercio:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';
  let producto = req.query.producto? req.query.producto:'';
  let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                     "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                     "barrio": new RegExp('^' + capitalizeWords(barrio)),
                     "codigo_merchant": new RegExp('^' + comercio)};
  dbo.collection("comercio").distinct("codigo_merchant", querycomercio, 
    (function(err, result) {
      console.log("result comereerr", result)
      if (err) console.log(err);
      let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
      dbo.collection("transaccion_log").aggregate([{$match: querytrx}, 
                                                   {$group: {_id: "$fecha_trx", "prom_trx":{$avg: "$monto"}}}, 
                                                   {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
      });
    }));
  }

  exports.obtener_promedio_trx = function(req, res, next, res_function){
    var dbo=mongo_connection.dbo;
    let provincia = req.query.provincia? req.query.provincia:'';
    let comercio = req.query.comercio? req.query.comercio:'';
    let ciudad = req.query.ciudad? req.query.ciudad:'';
    let barrio = req.query.barrio? req.query.barrio:'';
    let producto = req.query.producto? req.query.producto:'';
    let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                       "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                       "barrio": new RegExp('^' + capitalizeWords(barrio)),
                       "codigo_merchant": new RegExp('^' + comercio)};
    dbo.collection("comercio").distinct("codigo_merchant", querycomercio, 
      (function(err, result) {
        if (err) console.log(err);
        let querytrx= {"idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
        dbo.collection("transaccion_log").aggregate([{$match: querytrx}, 
                                                     {$group: {_id: "$fecha_trx", "prom_trx":{$avg: "$monto"}}}, 
                                                     {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
  
          if (err2) console.log(err2);
          res_function(null,resulttrx, req, res, next);
        });
      }));
    }


  exports.obtener_n_trx_n_periodo = function(req, res, next, res_function){
  var dbo=mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let comercio = req.query.comercio? req.query.comercio:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';
  let producto = req.query.producto? req.query.producto:'';
  let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                     "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                     "barrio": new RegExp('^' + capitalizeWords(barrio)),
                     "codigo_merchant": new RegExp('^' + comercio)};
  dbo.collection("comercio").distinct("codigo_merchant", querycomercio, 
    (function(err, result) {
      if (err) console.log(err);
      let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
      dbo.collection("transaccion_log").aggregate([{$match: querytrx}, 
                                                   {$group: {_id: "$fecha_trx", "n_trx":{$sum: 1}}}, 
                                                   {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
      });
    }));
  }

  function calcular_promedio(ventas, attr){
    var suma=0;
    for(var i=0; i<ventas.length; i++){
      suma+=parseFloat(ventas[i][attr].toString());
    }
    return {"prom_por_comercios": (suma/ventas.length)}
  }

  exports.obtener_promedio_trx_n_periodo_por_comercio = function(req, res, next, res_function){
    var dbo=mongo_connection.dbo;
    let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
    let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
    let provincia = req.query.provincia? req.query.provincia:'';
    let comercio = req.query.comercio? req.query.comercio:'';
    let ciudad = req.query.ciudad? req.query.ciudad:'';
    let barrio = req.query.barrio? req.query.barrio:'';
    let producto = req.query.producto? req.query.producto:'';
    let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                       "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                       "barrio": new RegExp('^' + capitalizeWords(barrio)),
                       "codigo_merchant": new RegExp('^' + comercio)};
    dbo.collection("comercio").distinct("codigo_merchant", querycomercio, 
      (function(err, result) {
        console.log("result comereerr", result)
        if (err) console.log(err);
        let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
        dbo.collection("transaccion_log").aggregate([{$match: querytrx}, 
                                                     {$group: {_id: "$idcomercio", "prom_trx":{$avg: "$monto"}}}, 
                                                     {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
  
          if (err2) console.log(err2);
          console.log("resultado por comercioo", resulttrx);
          var new_result=calcular_promedio(resulttrx, "prom_trx");
          res_function(null,new_result, req, res, next);
        });
      }));
    }


    exports.obtener_informacion_por_locacion = function(req, res, next, res_function){
      //no funciona
      var dbo=mongo_connection.dbo;
      let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
      let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
      let provincia = req.query.provincia? req.query.provincia:'';
      let comercio = req.query.comercio? req.query.comercio:'';
      let ciudad = req.query.ciudad? req.query.ciudad:'';
      let barrio = req.query.barrio? req.query.barrio:'';
      let producto = req.query.producto? req.query.producto:'';
      
      let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "producto":new RegExp('^' + producto)};
      dbo.collection('transaccion_log').aggregate([
        {
          $lookup: {
            from: 'comercio',
            localField: 'idcomercio',
            foreignField: 'codigo_merchant',
            as: 'comercio'
          }
        },
        {
          $unwind: '$comercio'
        }
      ]).toArray(function(err2, resulttrx) {
              if (err2) console.log(err2);
              console.log("resultado por comercioo", resulttrx);
              res_function(null,resulttrx, req, res, next);  
            })
      }