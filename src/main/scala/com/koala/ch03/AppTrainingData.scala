package com.koala.ch03

import com.koala.util.AppConst
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by haixiang on 2016/9/24.
  * package name~app name~class~key words~introduction
  * com.wandafilm.app~万达电影~购物优惠~团购|优惠|购物|电影~【/wp 关于/p 万达/nz 电影/n 】/wp
  * com.bbgz.android.app~宝贝格子~购物优惠~购物|海淘~【/wp 宝贝/n 格子/n 团队/n 精心/d 打造/v 】/wp
  * input: participles data
  * output: libsvm data.
  * mode: yarn-client, yarn-cluster or local[*].
  * 2rd_data/ch03/appdata.txt output/ch03/libsvmdata local[2]
  */

object AppTrainingData {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //
    val Array(input, output, mode) = args
    val sc = new SparkContext(new SparkConf().setMaster(mode).setAppName(this.getClass.getName))
    val rdd = sc.textFile(input)
      .map(_.split("~", -1))
      .map{ case terms =>
          //package name, app name, class, key words, introduction.
          (terms(0), terms(1), terms(2), terms(3), terms(4))}
      .map{ case (panme, aname, c, kw, intro) =>
           val introflt = intro.split(" ").map(_.split("/")).filter(x => x(0).length > 1 && filterProp(x(1))).map(x => x(0))
          (panme, aname, c, introflt)
    }.map(x => (x._3, x._4))

    //编码格式转换
    val minDF = rdd.flatMap(_._2.distinct).distinct()
    val indexes = minDF.collect().zipWithIndex.toMap
    //
    val training = rdd.repartition(4).map{
      case (label, terms) =>
        val svm = terms.map(v => (v, 1)).groupBy(_._1)
          .map{case (v, vs) => (v, vs.length)}
          .map{case (v, cnt) => (indexes.get(v).getOrElse(-1) + 1, cnt)}
          .filter(_._1 > 0)
          .toSeq
          .sortBy(_._1)
          .map(x => "" + x._1 + ":" + x._2)
          .mkString(" ")
        (AppConst.APP_CLASSES.indexOf(label), svm)
     }.filter(!_._2.isEmpty)
      .map(x => "" + x._1 + " " + x._2)

    training.coalesce(1).saveAsTextFile(output)
    sc.stop()
  }

  def filterProp(p:String):Boolean = {
    p.equals("v") || p.contains("n")
  }
}
