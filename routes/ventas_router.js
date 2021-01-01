let express = require('express');
let router = express.Router();
let util_autenticacion = require('../util_autenticacion');
let ventas_controller = require('../controller/ventas_controller');
let puntos_venta_controller = require('../controller/puntos_venta_controller');
let contbroadnet = require('../model/microsoftSQL/contbroadnet');


router.get('/obtener_vendedores', function (req, res, next) {
    contbroadnet.obtener_vendedores(req,res,next,null);
});

router.get('/obtener_ventas_n_periodo', ventas_controller.obtener_ventas_n_periodo);

router.get('/obtener_ventas_por_region', ventas_controller.obtener_ventas_por_region);

router.get('/obtener_ventas_por_provincia_periodo', ventas_controller.obtener_ventas_por_provincia_periodo);

router.get('/obtener_ventas_por_ciudad_periodo', ventas_controller.obtener_ventas_por_ciudad_periodo);

router.get('/obtener_ventas_por_proveedor_por_plataforma_periodo', ventas_controller.obtener_ventas_por_proveedor_por_plataforma_periodo);

router.get('/obtener_ventas_por_categoria_por_plataforma_periodo', ventas_controller.obtener_ventas_por_categoria_por_plataforma_periodo);

router.get('/obtener_ventas_por_modelo_negocio_periodo', ventas_controller.obtener_ventas_por_modelo_negocio_periodo);

router.post('/obtener_ventas_por_cuadrante', ventas_controller.obtener_ventas_por_cuadrante);

router.post('/obtener_numero_puntos_venta_mensual', puntos_venta_controller.obtener_numero_puntos_venta_mensual);

router.post('/obtener_numero_puntos_venta_nuevos_mensual', puntos_venta_controller.obtener_numero_puntos_venta_nuevos_mensual);

router.post('/obtener_numero_puntos_venta_anual', puntos_venta_controller.obtener_numero_puntos_venta_anual);

router.post('/obtener_numero_puntos_venta_nuevos_anual', puntos_venta_controller.obtener_numero_puntos_venta_nuevos_anual);

router.get('/verificar_permiso', function (req, res, next) {
    contbroadnet.verificar_existencia_permiso_usuario_metodo('usuario_ventas', 'GET', '/ventas/obtener_vendedores')
    .then(function (result) {
        res.json({"value": result});
    }).catch(function (err) {
        res.json({"error": err});
    });

});


router.get('/obtener_ventas', ventas_controller.obtener_ventas);

router.get('/obtener_ventas_etiqueta_proveedor', ventas_controller.obtener_ventas_etiqueta_proveedor);

module.exports = router;