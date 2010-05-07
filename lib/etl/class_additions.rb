class Class
def self.class_exists?(name)
    name.split(/::/).inject(Object) do |left, right|
        begin
            left.const_get(right)
        rescue NameError
            break nil
        end
    end
end
def self.class_with_name(name)
    if class_exists?(name)
        return Kernel.const_get(name)
    else
        return nil
    end
end
def is_kind_of_class(a_class)
    current = self
    while current do
        if current == a_class
            return true
        end
        current = current.superclass
    end
    return false
end

end

class Object
def is_kind_of_class(a_class)
	return self.class.is_kind_of_class(a_class)
end
end