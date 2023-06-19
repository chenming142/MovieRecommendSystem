// 查询电影数据集总数: 2791   
db.Movie.count();  

// 查询电影评分数据集总数: 44270
db.Rating.count();

// 查询电影标签数据集总: 456
db.Tag.count();


// 查询电影数据集详情
// mid: 电影ID  name: 电影名称 descri：电影描述 timelong：电影时长 issue:  shoot: 发行时间 language: 语言 genres: 类型 actors：演员 directors：导演
db.Movie.find({})
   .sort({_id:-1})
   .limit(100);
   
db.Rating.find({
    mid: 131168
}).limit(10);


// 评价最多的电影排序
db.Rating.aggregate([
    {
        $group: {_id: "$mid", count: {$sum: 1}}
    },
    {
        $sort: { count: -1 }
    }
]);


















































   