# 数据容器文件

import scrapy

class SpiderItem(scrapy.Item):
    pass

class XinwenxinxiItem(scrapy.Item):
    # 标题
    title = scrapy.Field()
    # 图片
    picture = scrapy.Field()
    # 媒体
    medianame = scrapy.Field()
    # 概述
    gaishu = scrapy.Field()
    # 发布时间
    pubtime = scrapy.Field()
    # 评论数
    commentcount = scrapy.Field()
    # 转发数
    repincount = scrapy.Field()
    # 点赞数
    likecount = scrapy.Field()
    # 来源
    laiyuan = scrapy.Field()

