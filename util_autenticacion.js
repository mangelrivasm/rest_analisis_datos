let jwt = require('jsonwebtoken');
let config = require('./config');
let dbreports = require('./model/microsoftSQL/dbreports');
exports.generateToken= function (payload) {
    return jwt.sign(payload,config.secret,{
        expiresIn: '10m'
    });
};

exports.authenticationMiddleware= function (req, res, next) {
    let usuario = req.body.usuario || req.query.usuario || req.headers['x-access-user'] || '';
    let token = req.body.token || req.query.token || req.headers['x-access-token'] || '';
    jwt.verify(token, config.secret, function(err, decoded) {
        if(err){
            res.status(403);
            res.json({"error": "Permiso denegado"});
        }else{
            let url = req.originalUrl;
            let operacion = req.method;
            dbreports.verificar_existencia_permiso_usuario_metodo(usuario, operacion, url).then(function (result) {
                if(result.length > 0){
                    next();
                }else{
                    let err = new Error('Permiso denegado!');
                    err.status = 403;
                    next(err);
                }
            }).catch(function (err) {
                err.status = 500;
                next(err);
            });
        }
    });
};