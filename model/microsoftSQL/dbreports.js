let ccserver_conection = require('./ccserver_conection');
let sql = require('mssql');

exports.verificar_credenciales_usuario = function (usuario, clave) {
    let request = ccserver_conection.ccserver_pool.request();
    request.input('usuario', sql.VarChar(50), usuario);
    request.input('clave', sql.VarChar(20), clave);
    return new Promise(function(resolve, reject){
       request.execute('DBREPORTS.dbo.analisis_datos_verificar_credenciales_usuario',
           function (err, result, returnValue, affected) {
               if(err){
                   reject(err);
               }else{
                   resolve(result.recordset);
               }
           }
       );
    });
};

exports.verificar_existencia_permiso_usuario_metodo = function(usuario, operacion, url){
    let request = ccserver_conection.ccserver_pool.request();
    request.input('nombre_usuario', sql.VarChar(50), usuario);
    request.input('operacion', sql.VarChar(10), operacion);
    request.input('url', sql.VarChar(100), url);
    return new Promise(function (resolve, reject) {
        request.execute('DBREPORTS.dbo.analisis_datos_verificar_existencia_permiso_usuario_metodo',
            function(err, result, returnValue, affected) {
                if(err){
                    reject(err);
                }else{
                    return resolve(result.recordset);
                }
            }
        );
    });

};