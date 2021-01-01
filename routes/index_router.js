let express = require('express');
let router = express.Router();
let dbreports = require('../model/microsoftSQL/dbreports');
let util_autenticacion = require('../util_autenticacion');

router.get('/', function (req, res, next) {
   res.json({"descripcion": "API REST ANALISIS DE DATOS", "version": "1.0.0"})
});

router.post('/autenticar', function (req, res, next) {
    let usuario = req.body.usuario;
    let clave = req.body.clave;
    dbreports.verificar_credenciales_usuario(usuario, clave).then(function (result) {
        if(result.length === 1){
            let usuario = result[0]["nombre_usuario"];
            let token = util_autenticacion.generateToken({"usuario":usuario, "clave": clave});
            res.json({"usuario": usuario ,"token": token});
        }else{
            res.status(403);
            res.json({"error": "usuario o clave incorrecta"})
        }
    }).catch(function (error) {
        res.status(403);
        res.json({"error": "Error de autenticacion"})
    });
});

module.exports = router;