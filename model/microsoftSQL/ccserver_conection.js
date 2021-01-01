const sql = require('mssql');
const config= require('../../config');

const ccserver_pool_config = {
    user: config.user_db_ccerver,
    password: config.password_db_ccserver,
    server: config.ccserver,
    encrypt: false,
    pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000
    }
};



const ccserver_pool = new sql.ConnectionPool(ccserver_pool_config, function (err) {
    if(!err){
        console.log("microsoftSQL: ",config.ccserver);
    }else{
        console.log("ERROR EN LA CREACION DEL POOL: ",err);
    }
});

exports.ccserver_pool = ccserver_pool;