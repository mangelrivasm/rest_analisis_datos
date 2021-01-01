let ccserver_conection = require('./ccserver_conection');
let sql = require('mssql');

exports.obtener_ruta_ejecutivo_por_dia = function (dia, id_ejecutivo) {
    let request = ccserver_conection.ccserver_pool.request();
    request.input('dia', sql.Int, dia);
    request.input('id_ejecutivo', sql.Int, id_ejecutivo);
    return new Promise(function (resolve, reject) {
        request.execute('DBGESTION.dbo.obtener_ruta_ejecutivo_por_dia',
            function(err, result, returnValue, affected) {
                if(err){
                    reject(err);
                }else{
                    return resolve(result["recordsets"][0]);
                }
            }
        );
    });
};