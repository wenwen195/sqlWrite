var fs = require("fs");
//将原文件中的精度降低，取小数点前五位，加快计算速度，减小分析结果大小
//获得原数据
fs.readFile('../../public/tmp/block/enCounty.json',(err, data) => {
    if(err) throw err;
    var inputs = JSON.parse(data);
    //循环获得每一个街区的特征
    for (var i = 0; i < inputs.features.length; i++) {
      //获得属性特征
      var attributes = inputs.features[i].attributes;
      //处理属性里中心坐标点的精度
      attributes.cy = new Number(attributes.cy.toFixed(5));//
      attributes.cx = new Number(attributes.cx.toFixed(5));//

      //获得空间数据特征geometry.rings闭合边界环的值
      var rings = inputs.features[i].geometry.rings;
      //循环获得rings中的所有闭合环
      for (var j = 0; j < rings.length; j++) {
        //循环获得每个闭合环的所有坐标点
        for (var k = 0; k < rings[j].length; k++) {
          //处理边界坐标点的精度
          rings[j][k][0] = new Number(rings[j][k][0].toFixed(5));//
          rings[j][k][1] = new Number(rings[j][k][1].toFixed(5));//
        }
      }
      //修改数据对象
      inputs.features[i].attributes = attributes;
      inputs.features[i].geometry.rings = rings;
    }
      //将处理后的对象写入新文件
      fs.writeFile('../../public/tmp/block/newEnCounty.json', JSON.stringify(inputs), (err) => {
        if (err) throw err;
        console.log("saved");
      })
});
