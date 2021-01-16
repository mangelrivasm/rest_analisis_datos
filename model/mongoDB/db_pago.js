const { query } = require('express');
let mongo_connection = require('./connection');

function capitalizeWords(word){
  word=word.toLowerCase();
  let finalSentence = word.replace(/(^\w{1})|(\s+\w{1})/g, letter => letter.toUpperCase())
  return finalSentence;
}

exports.obtener_pagos = function (req, res, next, res_function) {
    var dbo=mongo_connection.dbo;
    dbo.collection("pagocomercio").find().toArray(function(err, result) {
        if (err) console.log(err);
        res_function(null,result, req, res, next);
    });
  };

exports.obtener_n_pago_n_periodo = function(req, res, next, res_function){
    var dbo=mongo_connection.dbo;
    let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
    let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
    let provincia = req.query.provincia? req.query.provincia:'';
    let comercio = req.query.comercio? req.query.comercio:'';
    let ciudad = req.query.ciudad? req.query.ciudad:'';
    let barrio = req.query.barrio? req.query.barrio:'';
    let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                       "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                       "barrio": new RegExp('^' + capitalizeWords(barrio)),
                       "codigo_merchant": new RegExp('^' + comercio)};
    dbo.collection("comercio").distinct("id", querycomercio, 
      (function(err, result) {
        if (err) console.log(err);
        console.log("RESULTTTT", result)
        let querypagocomercio= {"fecha_deposito": { $lt : fecha_fin, $gte: fecha_inicio}, "id_comercio": { $in: result}};
        dbo.collection("pagocomercio").aggregate([{$match: querypagocomercio}, 
                                                     {$group: {_id: "$fecha_deposito", "n_pagos":{$sum: 1}}}, 
                                                     {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
  
          if (err2) console.log(err2);
          res_function(null,resulttrx, req, res, next);
        });
      }));
    }



  exports.obtener_promedio_pago_n_periodo = function(req, res, next, res_function){
      var dbo=mongo_connection.dbo;
      let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
      let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
      let provincia = req.query.provincia? req.query.provincia:'';
      let comercio = req.query.comercio? req.query.comercio:'';
      let ciudad = req.query.ciudad? req.query.ciudad:'';
      let barrio = req.query.barrio? req.query.barrio:'';
      let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                         "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                         "barrio": new RegExp('^' + capitalizeWords(barrio)),
                         "codigo_merchant": new RegExp('^' + comercio)};
      dbo.collection("comercio").distinct("id", querycomercio, 
        (function(err, result) {
          if (err) console.log(err);
          console.log("RESULTTTT", result)
          let querytrx= {"fecha_deposito": { $lt : fecha_fin, $gte: fecha_inicio}, "id_comercio": { $in: result}};
          dbo.collection("pagocomercio").aggregate([{$match: querytrx}, 
                                                       {$group: {_id: "$fecha_deposito", "prom_pago":{$avg: "$valor"}}}, 
                                                       {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
    
            if (err2) console.log(err2);
            console.log("PROMEDIOOOO", resulttrx);
            res_function(null,resulttrx, req, res, next);
          });
        }));
      }

