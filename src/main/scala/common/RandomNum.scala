package common

import scala.util.Random

/**
  * Created by anluyao on 2020-08-01 15:38
  */
class RandomNum {
  /**
    * 生成两位随机数
    *
    * @return
    */
  def randomNum(): String = {
    val randTwo = new Random().nextInt(90) + 10
    randTwo.toString
  }
}

object RandomNum {
  val instance: RandomNum = new RandomNum()

  def get = instance
}
