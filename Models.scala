package com.zhengkw.onlineDemo.beans

case class LearnModel(
                       userId: Int,
                       cwareId: Int,
                       videoId: Int,
                       chapterId: Int,
                       edutypeId: Int,
                       subjectId: Int,
                       sourceType: String,
                       speed: Int,
                       ts: Long,
                       te: Long,
                       ps: Int,
                       pe: Int
                     )

