#include <iostream>

class Foo
{
private:
    int a;
    double b;

public:
    Foo(int aa, int bb) : a(bb), b(aa) {}
};

int main()
{
    Foo f(1, 2);
}