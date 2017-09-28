package Init

trait Pattern {

    import scala.util.control.Exception._

    type closeAble={def close()}
    type close={def stop()}

    def using[R <:closeAble, A](resource: R)(f: R => A): A ={
      try {
        f(resource)
      } finally{
        //将这个catch逻辑应用于提供的主体
        //检查一个值是否包含在函数的域中。
        ignoring(classOf[Throwable]) apply resource.close()
      }
    }

    def useContext[R <:close, B](resource: R)(f: R => B): B ={
      try {
        f(resource)
      } finally{
        ignoring(classOf[Throwable]) apply resource.stop()
      }
  }

}