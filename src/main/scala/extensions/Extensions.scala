package extensions

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

object Extensions {

  implicit class SerializableExtensions(val self: Serializable) extends AnyVal {
    def save(path: String): Unit = {
      SerializableUtils.save(self, path)
    }
  }

  implicit class VectorExtensions(val self: Array[Float]) extends AnyVal {
    def average(other: Array[Float]): Array[Float] = {
      other.length match {
        case 0 => self
        case _ => self.zip(other).map { case (a, b) => 0.5f * (a + b) }
      }
    }

    def norm: Double = {
      math.sqrt(self.foldLeft(0.0f)((n, value) => n + value * value))
    }

    def dot(other: Array[Float]): Float = {
      self.zip(other).map { case (a, b) => a + b }.foldLeft(0.0f)((sum, value) => sum + value)
    }

    def cosineSimilarity(other: Array[Float]): Double = {
      self.dot(other) / (self.norm * other.norm)
    }
  }

  object SerializableUtils {
    def load[A](path: String): A = {
      val ois = new ObjectInputStream(new FileInputStream(path))
      ois.readObject().asInstanceOf[A]
    }

    def save[A](obj: A, path: String): Unit = {
      val oos = new ObjectOutputStream(new FileOutputStream(path))
      oos.writeObject(obj)
      oos.close()
    }
  }

}
