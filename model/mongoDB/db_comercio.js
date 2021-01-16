const { query } = require('express');
let mongo_connection = require('./connection');

function capitalizeWords(word){
  word=word.toLowerCase();
  let finalSentence = word.replace(/(^\w{1})|(\s+\w{1})/g, letter => letter.toUpperCase())
  return finalSentence;
}

exports.obtener_comercios = function (req, res, next, res_function) {
    var dbo=mongo_connection.dbo;
    dbo.collection("comercio").find().toArray(function(err, result) {
        if (err) console.log(err);
        res_function(null,result, req, res, next);
      });
};

exports.obtener_comercios_por_locacion = function(req, res, next, res_function){
    var dbo=mongo_connection.dbo;
    let provincia = req.query.provincia? req.query.provincia:'';
    let comercio = req.query.comercio? req.query.comercio:'';
    let ciudad = req.query.ciudad? req.query.ciudad:'';
    let barrio = req.query.barrio? req.query.barrio:'';
    let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                       "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                       "barrio": new RegExp('^' + capitalizeWords(barrio)),
                       "codigo_merchant": new RegExp('^' + comercio)};
    dbo.collection("comercio").find(querycomercio).toArray(function(err2, resulttrx) {
          if (err2) console.log(err2);
          console.log("comerciooos", resulttrx);
          res_function(null,resulttrx, req, res, next);
        });
      };
    
  exports.obtener_comercios_por_fecha_creacion = function(req, res, next, res_function){
        var dbo=mongo_connection.dbo;
        let fecha_creacion = req.query.fecha_creacion? req.query.fecha_creacion: '';
        let provincia = req.query.provincia? req.query.provincia:'';
        let comercio = req.query.comercio? req.query.comercio:'';
        let ciudad = req.query.ciudad? req.query.ciudad:'';
        let barrio = req.query.barrio? req.query.barrio:'';
        let querycomercio={"provincia": new RegExp('^' + provincia.toUpperCase()), 
                           "ciudad": new RegExp('^' + ciudad.toUpperCase()), 
                           "barrio": new RegExp('^' + capitalizeWords(barrio)),
                           "codigo_merchant": new RegExp('^' + comercio),
                           "fMrContrSignDate": fecha_creacion};
        dbo.collection("comercio").find(querycomercio).toArray(function(err2, resulttrx) {
              if (err2) console.log(err2);
              console.log("comerciooos", resulttrx);
              res_function(null,resulttrx, req, res, next);
            });
          };
      
    
    exports.obtener_rfm = function(req, res, next, res_function){
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
                                                    {$group: {_id: "$id_comercio", 
                                                              "ultimo_deposito":{$max: "$fecha_deposito"},
                                                              "numero_deposito":{$sum: 1},
                                                              "avg_deposito":{$avg: "$valor"}}}, 
                                                    {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
    
            if (err2) console.log(err2);
            console.log("RESULTADOOO FINAL", resulttrx);
            res_function(null,resulttrx, req, res, next);
          });
        }));
    }

    function getRFMAverage(rfm_comercios){
      var total_avg_deposito = 0;
      var total_days = 0;
      var total_numerodeposito = 0;
      for(var i=0; i<rfm_comercios.length; i++){
        total_avg_deposito+=parseFloat(rfm_comercios[i].avg_deposito.toString());
        total_days+=rfm_comercios[i].days;
        total_numerodeposito+=rfm_comercios[i].numero_deposito;
      }
      for(var j=0; j<rfm_comercios.length; j++){
        rfm_comercios[j]["percent_avg_deposito"]=(rfm_comercios[j].avg_deposito/total_avg_deposito)*100;
        rfm_comercios[j]["percent_numero_deposito"]=(rfm_comercios[j].numero_deposito/total_numerodeposito)*100;
        rfm_comercios[j]["percent_days"]=(rfm_comercios[j].days/total_days)*100;     
      }

      for(var k=0; k<rfm_comercios.length; k++){
        rfm_comercios[k]["monetary"]=getNumber(rfm_comercios[k].percent_avg_deposito);
        rfm_comercios[k]["frequency"]=getNumber(rfm_comercios[k].percent_numero_deposito);
        rfm_comercios[k]["recency"]=getNumberRecency(rfm_comercios[k].percent_days);  
      }

    }


    function getRFM(rfm_comercios){
      var numerodecomercios=rfm_comercios.length;
      rfm_comercios.sort(function(a, b) {
        return b.avg_deposito-a.avg_deposito;
      }); 
      
      var porcentaje=100/numerodecomercios;
      var counter=-porcentaje;
      for (var i=0; i<rfm_comercios.length; i++) {
        counter+=porcentaje;
        if(counter<20)
          rfm_comercios[i]["monetary"]=5;
        else{
          if(counter<40)
            rfm_comercios[i]["monetary"]=4;
          else{
            if(counter<60)
              rfm_comercios[i]["monetary"]=3;
            else{
              if(counter<80)
                rfm_comercios[i]["monetary"]=2;
               else{
                rfm_comercios[i]["monetary"]=1;
               }
            }
          }
        }
      }


      rfm_comercios.sort(function(a, b) {
        return b.numero_deposito-a.numero_deposito;
      }); 
      
      counter=-porcentaje;
      for (i=0; i<rfm_comercios.length; i++) {
        counter+=porcentaje;
        if(counter<20)
          rfm_comercios[i]["frequency"]=5;
        else{
          if(counter<40)
            rfm_comercios[i]["frequency"]=4;
          else{
            if(counter<60)
              rfm_comercios[i]["frequency"]=3;
            else{
              if(counter<80)
                rfm_comercios[i]["frequency"]=2;
               else{
                rfm_comercios[i]["frequency"]=1;
               }
            }
          }
        }
      }

      rfm_comercios.sort(function(a, b) {
        return b.days-a.days;
      }); 
      
      counter=-porcentaje;
      for (i=0; i<rfm_comercios.length; i++) {
        counter+=porcentaje;
        if(counter<20)
          rfm_comercios[i]["recency"]=1;
        else{
          if(counter<40)
            rfm_comercios[i]["recency"]=2;
          else{
            if(counter<60)
              rfm_comercios[i]["recency"]=3;
            else{
              if(counter<80)
                rfm_comercios[i]["recency"]=4;
               else{
                rfm_comercios[i]["recency"]=5;
               }
            }
          }
        }
      }
    }


    function getDays(rfm_comercios, fechafin){
      var new_rfm_comercios=[];
      var pattern = /(\d{4})(\d{2})(\d{2})/;
      for(var i=0; i<rfm_comercios.length; i++){
        var date = new Date(rfm_comercios[i].ultimo_deposito.replace(pattern,'$1-$2-$3'));
        var datefin = new Date(fechafin.replace(pattern,'$1-$2-$3'));
        var time_difference = datefin.getTime() - date.getTime();
        var days_difference = time_difference / (1000 * 60 * 60 * 24); 
        rfm_comercios[i]["days"]=days_difference;
        new_rfm_comercios.push(rfm_comercios[i]);
      }
      return new_rfm_comercios;
    }




    exports.obtener_grupo_rfm = function(req, res, next, res_function){
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
                                                    {$group: {_id: "$id_comercio", 
                                                              "ultimo_deposito":{$max: "$fecha_deposito"},
                                                              "numero_deposito":{$sum: 1},
                                                              "avg_deposito":{$avg: "$valor"}}},
                                                    {$sort: {_id: -1}}]).toArray(function(err2, resulttrx) {
    
            if (err2) console.log(err2);
            var result2=getDays(resulttrx, fecha_fin);
            getRFM(result2);
            res_function(null,result2, req, res, next);
          });
        }));
    }

