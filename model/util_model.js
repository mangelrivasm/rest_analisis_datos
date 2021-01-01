exports.formato_dinero=function(valor){
    return Math.round(valor*100)/100;
};

exports.validar_periodo_diario = function (fecha_inicio, fecha_fin) {
    try{
        if (!fecha_inicio || !fecha_fin) {
            return {"numero_meses": -1};
        }else if(fecha_inicio.length > 8 || fecha_fin.length > 8){
            return {"numero_meses": -1};
        }
        else{
            let numero_fecha_inicio = parseInt(fecha_inicio);
            let numero_fecha_fin = parseInt(fecha_fin);
            if(!numero_fecha_inicio || !numero_fecha_fin ||  numero_fecha_fin < numero_fecha_inicio){
                return {"numero_meses": -1};
            }else{
                let year        = fecha_inicio.substring(0,4);
                let month       = fecha_inicio.substring(4,6);
                let day         = fecha_inicio.substring(6,8);
                let fechaInicio        = new Date(year, month-1, day).getTime();
                year        = fecha_fin.substring(0,4);
                month       = fecha_fin.substring(4,6);
                day         = fecha_fin.substring(6,8);
                let fechaFin    = new Date(year, month-1, day).getTime();

                let numero_dias = (fechaFin - fechaInicio) / (1000*60*60*24);

                return {"numero_dias": numero_dias,
                    "texto_fecha_inicio": fecha_inicio,
                    "texto_fecha_fin": fecha_fin};
            }
        }
    }catch (err){
        return {"numero_meses": -1};
    }
};

exports.validar_periodo_mensual = function (fecha_inicio, fecha_fin) {
    try{
        if (!fecha_inicio || !fecha_fin) {
            return {"numero_meses": -1};
        }else if(fecha_inicio.length > 6 || fecha_fin.length > 6){
            return {"numero_meses": -1};
        }
        else{
            let numero_fecha_inicio = parseInt(fecha_inicio);
            let numero_fecha_fin = parseInt(fecha_fin);
            if(!numero_fecha_inicio || !numero_fecha_fin ||  numero_fecha_fin < numero_fecha_inicio){
                return {"numero_meses": -1};
            }else{
                let anio_inicio = parseInt(fecha_inicio.substr(0, 4));
                let mes_inicio = parseInt(fecha_inicio.substr(4, 2));
                let anio_fin = parseInt(fecha_fin.substr(0, 4));
                let mes_fin = parseInt(fecha_fin.substr(4, 2));
                if(mes_inicio > 0 && mes_inicio <= 12 && mes_fin > 0 && mes_fin <=12 ) {
                    let numero_meses = (anio_fin - anio_inicio) * 12 + (mes_fin - mes_inicio) +1;
                    let dia_fin = new Date(anio_fin, mes_fin, 0).getDate();
                    return {"numero_meses": numero_meses,
                            "texto_fecha_inicio": fecha_inicio+'01',
                            "texto_fecha_fin": fecha_fin+dia_fin};
                }else{
                    return {"numero_meses": -1};
                }
            }
        }
    }catch (err){
        return {"numero_meses": -1};
    }
};

exports.validar_periodo_anual = function (fecha_inicio, fecha_fin) {
    try{
        if (!fecha_inicio || !fecha_fin) {
            return {"numero_anios": -1};
        }else if(fecha_inicio.length > 4 || fecha_fin.length > 4){
            return {"numero_anios": -1};
        }
        else{
            let numero_fecha_inicio = parseInt(fecha_inicio);
            let numero_fecha_fin = parseInt(fecha_fin);
            if(!numero_fecha_inicio || !numero_fecha_fin ||  numero_fecha_fin < numero_fecha_inicio){
                return {"numero_anios": -1};
            }else{
                let anio_inicio = parseInt(fecha_inicio.substr(0, 4));
                let anio_fin = parseInt(fecha_fin.substr(0, 4));
                let numero_anios = anio_fin - anio_inicio +1;
                return {"numero_anios": numero_anios,
                    "texto_fecha_inicio": fecha_inicio+"0101",
                    "texto_fecha_fin": fecha_fin+"1231"};
            }
        }
    }catch (err){
        return {"numero_anios": -1};
    }
};

exports.obtener_limites_fechas_n_meses = function (n) {
    let fecha_actual = new Date();
    let fecha_inicio = new Date();
    fecha_actual.setMonth(fecha_actual.getMonth() - 1);
    fecha_inicio.setMonth(fecha_inicio.getMonth() - n);
    let anio_fin = fecha_actual.getFullYear(), mes_fin = fecha_actual.getMonth()+1;
    let anio_inicio = fecha_inicio.getFullYear(), mes_inicio = fecha_inicio.getMonth()+1;
    let texto_fecha_inicio = ""+anio_inicio;
    if(mes_inicio < 10)
        texto_fecha_inicio = texto_fecha_inicio + "0"+mes_inicio;
    else
        texto_fecha_inicio = texto_fecha_inicio + mes_inicio;

    let texto_fecha_fin = ""+anio_fin;
    if(mes_fin < 10)
        texto_fecha_fin = texto_fecha_fin + "0"+mes_fin;
    else
        texto_fecha_fin = texto_fecha_fin + mes_fin;
    return {"fecha_inicio": texto_fecha_inicio, "fecha_fin": texto_fecha_fin};
};

exports.obtener_limites_fechas_n_anios = function (n) {
    let fecha_actual = new Date();
    let fecha_inicio = new Date();
    fecha_actual.setFullYear(fecha_actual.getFullYear() - 1);
    let anio_fin = fecha_actual.getFullYear();
    fecha_inicio.setFullYear(fecha_inicio.getFullYear() - n);
    let anio_inicio = fecha_inicio.getFullYear();
    return {"fecha_inicio": anio_inicio+"01", "fecha_fin": anio_fin+"12"};
};