package fr.ub.m2gl;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

public class TileManager extends ResourceConfig {

    public TileManager() {
        // Register resources and providers using package-scanning.
        packages("fr.ub.m2gl");

        register(TileMapperProvider.class);

        // Enable Tracing support.
        property(ServerProperties.TRACING, "ALL");
    }
}
