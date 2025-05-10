#include <memory>
#include <iostream>
#include <vector>
#include <cassert>

class Inner {
    public:
    Inner() {
        std::cout << "Inner construction" << std::endl;
    }
    ~Inner() {
        std::cout << "Inner destruction" << std::endl;
    }
    void hello() { 
        std::cout << "Hello" << std::endl;
    }
    int x;
};

class Outer {
    public:
    Outer() {
        std::cout << "Outer construction" << std::endl;
    }
    ~Outer() {
        std::cout << "Outer destruction" << std::endl;
    }
    std::vector<std::unique_ptr<Inner>> myvec;
};

class OuterShared {
    public:
    OuterShared() {
        std::cout << "Outshared constructor" << std::endl;
    }
    ~OuterShared() {
        std::cout << "OuterChared Destructor" << std::endl;
    }
    std::vector<std::shared_ptr<Inner>> myvec;
};

int main() {
    std::cout << "Begin test" << std::endl;

    {
        OuterShared o2;
        o2.myvec.push_back(std::make_unique<Inner>());
        o2.myvec.push_back(std::make_unique<Inner>());
        o2.myvec.push_back(std::make_unique<Inner>());
        {
            auto e1 = o2.myvec[0];
            e1->hello();
        }

    }
    {
    Outer o;
    o.myvec.push_back(std::make_unique<Inner>());
    o.myvec.push_back(std::make_unique<Inner>());
    o.myvec.push_back(std::make_unique<Inner>());
        {
            auto e1 = std::move(o.myvec[0]);
            e1->hello();
        }
    }

    int x = 5;
    assert(x==7);
}