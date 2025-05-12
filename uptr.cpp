    #include <iostream>
    #include <memory>

    class ParquetStuff {
        private:
            int _x;
        public:
            ParquetStuff(int x): _x(x) 
            { std::cout << "Construct" << std::endl; }
            ParquetStuff(const ParquetStuff& other): _x(other._x) 
            { std::cout << "Copy" << std::endl; }
            ~ParquetStuff() 
            { std::cout << "Destruct" << std::endl; }
            int getX() 
            { return _x; }
            void setX(int newX) 
            { _x = newX; }
    };

    void reader(const std::unique_ptr<ParquetStuff>& ptr) {
        if (ptr) {
            std::cout << "Value: " << ptr->getX() << std::endl;
        }
    }

    void writer(std::unique_ptr<ParquetStuff>& ptr) {
         if (ptr) {
            ptr->setX(ptr->getX() * 2);;
        }
    }
    int main() {
        std::unique_ptr<ParquetStuff> myPtr = std::make_unique<ParquetStuff>(27);
        reader(myPtr);
        writer(myPtr);
        if (myPtr) {
            std::cout << "Value after modify: " << myPtr->getX() << std::endl; 
        }
        return 0;
    }