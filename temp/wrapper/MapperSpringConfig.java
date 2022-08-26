package sky.wrapper;

import org.mapstruct.MapperConfig;
import org.mapstruct.extensions.spring.converter.ConversionServiceAdapter;

//@SpringMapperConfig(conversionServiceAdapterPackage = "org.mapstruct.extensions.spring.example.boot")
@MapperConfig(componentModel = "spring", uses = ConversionServiceAdapter.class)
public interface MapperSpringConfig {
}
