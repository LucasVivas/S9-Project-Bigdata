package ub.m2;

import javax.imageio.ImageIO;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
@Path("")
public class TileRessource {

    private TileDB tileDB;

    public TileRessource() throws IOException{
        tileDB = new TileDB();
    }

   @Path("/{z}/{x}/{y}")
   @GET
   @Produces("image/png")
   public Response getUserInJSON(@PathParam("z") int z, @PathParam("x") int x, @PathParam("y") int y) {
       BufferedImage img;
       byte [] tilesByte = tileDB.getTile(z,x,y);
           return Response.ok(tilesByte).build();
   }
}
