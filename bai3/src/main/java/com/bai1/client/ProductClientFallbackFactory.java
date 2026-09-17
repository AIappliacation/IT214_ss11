package com.bai1.client;

import com.bai1.dto.ProductInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class ProductClientFallbackFactory
        implements FallbackFactory<ProductClient> {

    private static final Logger log =
            LoggerFactory.getLogger(ProductClientFallbackFactory.class);

    @Override
    public ProductClient create(Throwable cause) {

        log.error(
                "Product Service call failed",
                cause
        );

        return new ProductClient() {

            @Override
            public ProductInfo getById(Long id) {

                log.warn(
                        "Fallback getById for product id={}",
                        id
                );

                return ProductInfo.fallback(id);
            }

            @Override
            public List<ProductInfo> getAll() {

                log.warn(
                        "Fallback getAll products"
                );

                return Collections.emptyList();
            }
        };
    }
}