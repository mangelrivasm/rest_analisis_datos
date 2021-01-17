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

exports.obtener_suma_trx_n_periodo = function(req, res, next, res_function){
  var dbo = mongo_connection.dbo;
  let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
  let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
  let provincia = req.query.provincia? req.query.provincia:'';
  let ciudad = req.query.ciudad? req.query.ciudad:'';
  let barrio = req.query.barrio? req.query.barrio:'';
  let producto = req.query.producto? req.query.producto:'';
  let periodo = req.query.periodo ? req.query.periodo : '';

  let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                     "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                     "barrio": new RegExp('^' + capitalizeWords(barrio))};
  dbo.collection("comercio").distinct("codigo_merchant", querycomercio, 
  (function(err, result) {
    console.log("result comereerr", result)
    if (err) console.log(err);
    let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
    
    
    if(periodo === "DIARIO")
    {

      dbo.collection("transaccion_log").aggregate([
        {
          $match: querytrx,
        }, 
        {
          $group: {
            _id: "$fecha_trx" , 
            "prom_trx":{$sum: "$monto"}
          }
        }, 
        {
          $sort: {_id: -1}
        }
      ])
      .toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
        });
    }
    else if(periodo === "MENSUAL")
    {
      
      dbo.collection("transaccion_log").aggregate([
        {
          $match: querytrx,
        }, 
        {
          $group: {
            _id: {$substr: ['$fecha_trx', 0, 6]} , 
            "prom_trx":{$sum: "$monto"},
            //"MonthValue": { "$first": "$DueDateMonth" }
          }
        }, 
        {
          $sort: {_id: -1}
        }
      ])
      .toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
        });
    }
    else if(periodo === "ANUAL")
    {
      dbo.collection("transaccion_log").aggregate([
        {
          $match: querytrx,
        }, 
        {
          $group: {
            _id: {$substr: ['$fecha_trx', 0, 4]} , 
            "prom_trx":{$sum: "$monto"},
            //"MonthValue": { "$first": "$DueDateMonth" }
          }
        }, 
        {
          $sort: {_id: -1}
        }
      ])
      .toArray(function(err2, resulttrx) {

        if (err2) console.log(err2);
        res_function(null,resulttrx, req, res, next);
        });
      
    }



    
    // dbo.collection("transaccion_log").aggregate([{$match: querytrx}, 
    //                                             {$group: {_id: "$fecha_trx" , "prom_trx":{$sum: "$monto"}}}, 
    //                                             {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {

    //   if (err2) console.log(err2);
    //   res_function(null,resulttrx, req, res, next);
    // });

    


  }));
}


// exports.obtener_promedio_trx_n_periodo = function(req, res, next, res_function){
//   var dbo=mongo_connection.dbo;
//   let fecha_inicio = req.query.fecha_inicio? req.query.fecha_inicio: '';
//   let fecha_fin = req.query.fecha_fin? req.query.fecha_fin: '' ;
//   let provincia = req.query.provincia? req.query.provincia:'';
//   let comercio = req.query.comercio? req.query.comercio:'';
//   let ciudad = req.query.ciudad? req.query.ciudad:'';
//   let barrio = req.query.barrio? req.query.barrio:'';
//   let producto = req.query.producto? req.query.producto:'';
//   let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
//                      "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
//                      "barrio": new RegExp('^' + capitalizeWords(barrio)),
//                      "codigo_merchant": new RegExp('^' + comercio)};
//   dbo.collection("comercio").distinct("codigo_merchant", querycomercio, 
//     (function(err, result) {
//       console.log("result comereerr", result)
//       if (err) console.log(err);
//       let querytrx= {"fecha_trx": { $lt : fecha_fin, $gte: fecha_inicio}, "idcomercio": { $in: result}, "producto":new RegExp('^' + producto)};
//       dbo.collection("transaccion_log").aggregate([{$match: querytrx}, 
//                                                    {$group: {_id: "$fecha_trx", "prom_trx":{$avg: "$monto"}}}, 
//                                                    {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {

//         if (err2) console.log(err2);
//         res_function(null,resulttrx, req, res, next);
//       });
//     }));
// }


