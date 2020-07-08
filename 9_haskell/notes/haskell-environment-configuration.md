Haskell 是一种标准化的, 通用的纯函数编程语言. 函数式编程的两个最大的特点是 No side-effects 和  Referential transparency, 前者指在函数式编程中, 一个函数唯一能做的事情就是计算一些东西然后将其返回为结果; 后者则指的是在函数式编程中, 如果一个函数以相同的参数被调用两次, 能够保证结果一定相同. 正因如此使编译器能理解函数的行为, 且能让你通过结果来判断函数是否正确, 从而通过融合多个函数一起来创造更复杂的函数. 

# Installation

在 Mac 上, 可以通过 Homebrew 方便地安装, Homebrew 安装的好处是断了重下会继续在上个节点开始.

```bash
brew install ghc // 编译器(Glasgow Haskell Compiler)
brew install cabal-install // 包管理
brew install haskell-stack // 集成工具
```

接着在命令行输入 `ghci` 就可以进行交互式编程了:

```bash
$ ghci
GHCi, version 8.10.1: https://www.haskell.org/ghc/  :? for help
Prelude>
```

提示信息 `Prelude` 可能会变长影响观感, 所以可以输入类似 Vim 的命令 `: set prompt "ghci> "` 来改变当前会话的 prompt.

### GHCI - Glasgow Haskell Compiler

从英文翻译而来-Glasgow Haskell编译器是功能性编程语言Haskell的开源本机代码编译器。它为Haskell代码的编写和测试提供了一个跨平台环境，并支持许多扩展，库和优化，从而简化了生成和执行代码的过程。GHC是最常用的Haskell编译器。 