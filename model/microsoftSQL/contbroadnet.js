let ccserver_conection = require('./ccserver_conection');
let sql = require('mssql');

exports.obtener_vendedores = function (req, res, next, res_function) {
    ccserver_conection.ccserver_pool.request().query("SELECT codvendedor id, nomvendedor nombre, codjefatura id_jefe " +
        "FROM CONTBROADNET.dbo.shvendedor WHERE estado = 'A' ORDER BY nomvendedor",
        function(err, result){
            if(!err){
                res.json({"vendedores": result.recordset});
            }else{
                next(err, req, res);
            }

    })
};