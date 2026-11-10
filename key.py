import hashlib
import re

# TODO: operands on Key should they return a key or a str ??
_HEX64_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_HEX_RE   = re.compile(r"^[0-9a-fA-F]+$")


class Key(object):
    '''
    keyを操作するためのクラス。sha256に寄った実装になってる。
    まだハッシュ化してない場合はUidクラスで初期化が必要。
    ・idlength(int):keyのbit長。16の倍数じゃないとダメ。
    ・value(str):key。16進数のstr。

    '''
    def __init__(self, value):
        # 256 because we use sha256 (64 hexa char)
        self.idlength = 256

        # 1) Key のコピーを許可（互換性UP）
        if isinstance(value, Key):
            self.value = value.value
            return

        # 2) int を 256bit に正規化して 64hex にする（互換性UP）
        if isinstance(value, int):
            v = value % pow(2, self.idlength)
            self.value = format(v, '0>{}x'.format(self.idlength // 4))
            return

        # 3) str の場合：64hexならそのまま、そうでなければ sha256 で 64hex化
        if isinstance(value, str):
            # すでに64hex(=256bit)なら従来通り
            if _HEX64_RE.match(value):
                self.value = value.lower()
                return

            # 「短いhex」や「任意文字列」は sha256 に写して 256bit 空間へ
            # （“値域が極端に小さい/偏る”問題を避ける）
            h = hashlib.sha256(value.encode("utf-8")).hexdigest()
            self.value = h
            return

        raise TypeError("Can create key only from str/int/Key")

    def setValue(self, newValue):
        '''
        keyの変更に使用
        '''
        if isinstance(newValue, str) and len(newValue) == self.idlength // 4:
            self.value = newValue
        else:
            raise ValueError

    def __repr__(self): #print()のときの動作。valueの10桁目まで表示
        if len(self.value)>10:
            return self.value[:9]
        else:
            return self.value

    # gt~ne:比較演算子用関数。Keyクラスとstr,key,intクラスの比較が可能。
    def __gt__(self, value):
        if isinstance(value, str):
            return int(self.value, 16) > int(value, 16)
        elif isinstance(value, Key):
            return int(self.value, 16) > int(value.value, 16)
        elif isinstance(value, int):
            return int(self.value, 16) > value
        else:
            raise TypeError("__gt__ only supports str, int or Key as input")

    def __ge__(self, value):
        if isinstance(value, str):
            return int(self.value, 16) >= int(value, 16)
        elif isinstance(value, Key):
            return int(self.value, 16) >= int(value.value, 16)
        elif isinstance(value, int):
            return int(self.value, 16) >= value
        else:
            raise TypeError("__ge__ only supports str or Key as input")

    def __lt__(self, value):
        if isinstance(value, str):
            return int(self.value, 16) < int(value, 16)
        elif isinstance(value, Key):
            return int(self.value, 16) < int(value.value, 16)
        elif isinstance(value, int):
            return int(self.value, 16) < value
        else:
            raise TypeError("__lt__ only supports str or Key as input")

    def __le__(self, value):
        if isinstance(value, str):
            return int(self.value, 16) <= int(value, 16)
        elif isinstance(value, Key):
            return int(self.value, 16) <= int(value.value, 16)
        elif isinstance(value, int):
            return int(self.value, 16) <= value
        else:
            raise TypeError("__le__ only supports str or Key as input")

    def __eq__(self, value):
        if isinstance(value, str):
            return int(self.value, 16) == int(value, 16)
        elif isinstance(value, Key):
            return int(self.value, 16) == int(value.value, 16)
        elif isinstance(value, int):
            return int(self.value, 16) == value
        else:
            raise TypeError("__eq__ only supports str or Key as input")

    def __ne__(self, value):
        if isinstance(value, str):
            return int(self.value, 16) != int(value, 16)
        elif isinstance(value, Key):
            return int(self.value, 16) != int(value.value, 16)
        elif isinstance(value, int):
            return int(self.value, 16) != value
        else:
            raise TypeError("__ne__ only supports str or Key as input")

    def canonicalize(self, value:int):
        # TODO: set as classmethod or function
        '''
        intを16進数に変換し、左ゼロ埋めしてn桁の16進文字列に。ただし冒頭の0xはない。
        Returns the str repr of hexa value with the right number of hexa char
        Basically padd the input with'0' and get rid of '0x' and 'L'
        '''
        return format(value, '0>{}x'.format(self.idlength // 4))

    # add~subint: 加算減算用。Keyクラスとstr,key,intクラスの計算が可能。返り値はstr型
    def __add__(self, value):
        if isinstance(value, int):
            return self.sumint(value)
        elif isinstance(value, str):
            return self.sumint(int(value, 16))
        elif isinstance(value, Key):
            return self.sumint(int(value.value, 16))
        else:
            #self.log.error("Sum with unknow type")
            print(type(value))
            raise TypeError

    def sumint(self, value:int):
        '''
        Return sum uid + value in hexa representation
        @param value: int to sum with uid value
        '''
        res = (int(self.value, 16) + value) % pow(2, self.idlength) #返り値は円環状にする
        return self.canonicalize(res) #16進数に変換して返す

    def __sub__(self, value):
        if isinstance(value, int):
            return self.subint(value)
        elif isinstance(value, str):
            return self.subint(int(value, 16))
        elif isinstance(value, Key):
            return self.subint(int(value.value, 16))
        else:
            #self.log.error("Sub with unknow type")
            raise TypeError

    def subint(self, value):
        '''
        Return sub uid - value in hexa representation
        @param value: int to sub with uid value
        '''
        res = (int(self.value, 16) - value) % pow(2, self.idlength)
        return self.canonicalize(res)

    def __len__(self):
        '''
        len()のときの挙動
        '''
        return len(self.value)

    #is_between_r_inclu~is_between_exclu: 自身のキーが一定区間内(環状)にあるか。区間になってない(limit1==limit2)とエラー。
    #limitはstr,key,intのどれか
    #内側にいるかの判定は基本_is_insideメソッドでやる。境界判定のみ別途やる
    def is_between_r_inclu(self, limit1, limit2):
        """
        limit1 < self.value <= limit2
        True if self.value is contained by (limit1, limit2]
        Return False otherwise
        Raise EqualLimitError if limit1 == limit2
        """
        if self.value == limit2:
            return True
        elif self.value == limit1:
            return False
        return self._is_inside(limit1, limit2)

    def is_between_l_inclu(self, limit1, limit2):
        """
        limit1 <= self.value < limit2
        True if self.value is contained by [limit1, limit2)
        Return False otherwise
        Raise EqualLimitError if limit1 == limit2
        """
        if self.value == limit1:
            return True
        elif self.value == limit2:
            return False
        return self._is_inside(limit1, limit2)

    def is_between_inclu(self, limit1, limit2):
        """
        使ってない。isbetweenと挙動は同じ
        limit1 <= self.value <= limit2
        True if self.value is contained by [limit1, limit2]
        Return False otherwise
        Raise EqualLimitError if limit1 == limit2
        """
        if self.value == limit2 or self.value == limit1:
            return True
        return self._is_inside(limit1, limit2)

    def is_between_exclu(self, limit1, limit2):
        """
        limit1 < self.value < limit2
        True if self.value is contained by (limit1, limit2)
        Return False otherwise
        Raise EqualLimitError if limit1 == limit2
        """
        if self.value == limit2 or self.value == limit1:
            return False
        return self._is_inside(limit1, limit2)

    def _is_inside(self, limit1, limit2):
        '''
        円環上に範囲内か範囲外かを判定する非公開メソッド。境界判定はしないため境界にself.valueがあるとerror
        limit1<self.value<limit2
        Returns True if self.value is contained by ]limit1,  limit2[
        Raise Exceptions otherwise
        If self.value == limit1 or self.value == limit2, raise ValueError
        Raise ValueError if limit1 == limit2
        '''
        if isinstance(limit1, Key):
            limit1 = limit1.value
        if isinstance(limit2, Key):
            limit2 = limit2.value
        
        # keyの長さを確かめる
        if not (len(self) == len(limit1) == len(limit2)): #len(self)==len(self.vlue)
            raise ValueError(
                "Unable to compare different length value and limit")
        
        v = int(self.value, 16)
        l1 = int(limit1, 16)
        l2 = int(limit2, 16)

        if v == l1 or v == l2:
            return False

        #内側にいるか
        if l1 > l2:
            return v > l1 or v < l2
        elif l1 < l2:
            return l1 < v < l2
        else:
            # limit1 == limit2: ring全体(全周)として扱う
            return True

    def isbetween(self, limit1, limit2):
        '''
        公開メソッド。limit1<=self.value<=limit2
        Returns True if self.value is contained by [limit1,  limit2]
        So if self.value == limit1 or limit2 then return True
        Raise exception if limit1 == limit2
        '''
        if not (len(self) == len(limit1) == len(limit2)):
            #self.log.error("Unable to compare.")
            raise ValueError(
                "Unable to compare different length value and limit")
        if self.value == limit1 or self.value == limit2:
            return True

        if limit1 > limit2:
            if self.value > limit1 or self.value < limit2:
                return True
            else:
                return False
        elif limit1 < limit2:
            if self.value > limit1 and self.value < limit2:
                return True
            else:
                return False
        else:
            # limit1 == limit2
            raise ValueError("isbetween: limit1 == limit2")


class Uid(Key):
    '''
    Keyクラスを継承し、keyのvalueとしてsha256を採用したもの。
    self.valueはsha256でstrtohash(str to hash=入力はstr)をhash化したものになる
    '''
    def __init__(self, strtohash:str):
        hash = hashlib.sha256(strtohash.encode("utf-8"))
        Key.__init__(self, hash.hexdigest())

class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class EqualLimitsError(Error):
    pass
