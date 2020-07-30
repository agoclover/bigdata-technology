### 伴生对象与伴生类

static 关键字修饰属性和方法属于类, 违背了面向对象的设计院长职责, 但又不能取消这种属于类的属性和方法, 所以底层通过单例对象处理, 将静态方法和属性都放在伴生对象中.

伴生对象其实是伴生对象所属类里面的单例对象. 所以 scala 中静态属性和方法都应该定义在同名 Object 中.

### scala 中定义变量

可以通过票号使用关键字和保留字, 可以使用符号定义方法.

### 字符串输出的三种格式

`println() / printf()`

`""" """`

`s"$a+$b"`

### 数据类型

引用类型包括 scala 自己的类型和 java 的类库.

几个特殊的类和对象:

Unit → `()` 

Nothing→ 一般用于标记异常, 是所有类型的子类.

Null → `null`

### 计算

scala 没有 `++`, 有 `+=`, 但底层不和 java 一样, 没有类型强转.

### 流程控制

分值控制: 分支有返回值, 每一个分支体最后一行的对象.

For 循环控制: 

- 类似 java 的增强 for 循环. 控制迭代的方式.

- `1 to 10` 返回 Range, 这里使用了隐式转换: `Predef` 
- 循环返回值通过 `yield` 接收.

循环中断:

- `Breaks.break` → 抛出异常
- `Breaks.breakable(opt : => Unit)` 处理异常
- `import .Breaks_` 静态导入以简化.

### 函数式编程

函数至简原则

高阶函数: 函数是一等公民.

**1 函数可以作为值进行传递**

**2 函数可以作为参数进行传递**

	- 解耦
	- 动态地扩展函数的功能

匿名函数: 

- 至简原则注意, 执行时会找离下划线最近的表达式进行还原.

- 元组加括号没用, 建议只用一个变量, 区别于模式匹配.

**3 函数可以作为函数返回值返回**

主要用于闭包, 使用闭包外层函数可以释放占内存, 区别于 java 的函数调用栈积压. 2.12版本后不落盘了, `$anonfun$`

函数柯里化, 简化闭包的编写, 让每个参数表达的含义更加清晰明确.

如果每一层功能不同 → 传入代码块 → 控制抽象, `myWhile` 循环: 普通递归, 闭包形式(函数嵌套), 柯里化.

### 面向对象

#### scala 包

1. 一个原文件可以定义多个包;
2. 子包中的对象可以直接访问父包的类型.

包对象

#### 类和对象

定义 @BeanProperty

修饰符

构造: 父主 → 父辅 → 子主 →子辅

继承 父主 → 父辅 → 子主 →子辅

抽象属性和方法

特质: 特质叠加, 特质和抽象类的区别

#### 模式匹配

泛型擦除, 集合有, 数组没有

`::` 添加元素 `:::` 扁平化

`unapply` 封装为 Option 的子类(`null` 和 `Some()`)

样例类构造器参数默认 `val` 类型, 普通类当做参数.

#### 隐式转换

#### 泛型

协变逆变

# spark

task 是怎么调度的

内存

`sortBy()` 转换算子有提交操作.



## java interviews

### final finally finallize 区别

final: 用于声明类属性方法，表示类不可继承，方法不可重载，属性不可变

finally: 用于Java异常处理语句结构，表示总是执行

finallize: 用于 Java 垃圾回收机制方法, 调用该方法处理 Java 内存垃圾, 例如关闭文件. 定义在 object 中, 调用但不会立即执行. 