// 查询电影数据集总数: 2791   
db.Movie.count();  

// 查询电影评分数据集总数: 44270
db.Rating.count();

// 查询电影标签数据集总: 456
db.Tag.count();


// 查询电影数据集详情
// mid: 电影ID  name: 电影名称 descri：电影描述 timelong：电影时长 issue:  shoot: 发行时间 language: 语言 genres: 类型 actors：演员 directors：导演
db.Movie.find({
    mid: 1395
})
   .sort({_id:-1})
   .limit(100);

// 分组排序查询
db.Movie.aggregate([
    {
        $group: {_id: "$mid", count: {$sum: 1}}
    },
    {
        $sort: { count: -1 }
    }
]);
   
   
   
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


db.Tag.find({
    mid: 1084
});

db.Tag.aggregate([
    { $match: { mid: 1025 } },
    {
        $group: {
            _id: "$mid",
            total: {$sum: 1},
            tags: {
                $push: {
                    $concat: ["$tag"]
                }
            }
        }
    },
    {
        $sort: { total: -1}
    }
]);


// 查询 monogdb 版本
db.version();


// 统计评分次数最多的电影
db.RateMoreMovies.count();

db.RateMoreMovies.find({
    mid: 296
}).limit(1);

db.RateMoreMovies.find({}).sort({
    count: -1
})

// 最近热门电影统计(按月)
db.RateMoreRecentlyMovies
    .find({})
    .sort({
        count: -1,
        yearmonth: -1
    })
    .limit(10);


// 电影平均得分统计
db.AverageMovies
    .find({})
    .sort({
        avg: -1
    })
    .limit(10);
    
// 每种类别优质电影统计
db.GenresTopMovies
    .find({});

// 基于用户的推荐
db.UserRecs.find({}).limit(100);

// 基于电影的推荐
db.MovieRecs
    .find({})
    .limit(100);

db.MovieRecs.aggregate([
    {
        $project: {
            cnt: {
                $size: "$recs"
            }
        }
    }
]);













































   