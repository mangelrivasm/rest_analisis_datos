
exports.response_json = function (err, result, req, res, next){
    if(err){
        next(err,req,res);
    }else{
        res.json(result);
    }
};