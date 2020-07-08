doubleMe x = x + x
doubleUs x y = doubleMe x + doubleMe y
doubleSmallNumber x = if x > 100
    then x
    else x * 2
doubleSmallNumber2 x = (if x > 100 then x else x * 2) + 1
doubleSmallNumber3 x = if x > 100 then x else x * 2 + 1
doubleSmallNumber' x = (if x > 100 then x else x*2) + 1
amos'Amos = "I'm Amos."
boomBangs numList = [if x < 10 then "smaller" else "bigger" | x <- numList, odd x]
myLC numList = [x | x <- numList, x /= 13, x /= 15, x /= 19]
length' xs = sum [1 | _ <- xs]
removeNonUppercase xs = [x | x <- xs, x `elem` ['A'..'Z']]
myLC2 ll = [ [ x | x <- l, odd x] | l <- ll]
myLC3 ll = [x | x + 2]