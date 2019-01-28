package fr.ub.m2gl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.JSONPObject;
import com.mongodb.client.FindIterable;
import org.bson.Document;
import org.glassfish.hk2.api.UseProxy;

import javax.imageio.ImageIO;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
@Path("")
public class TileRessource {

   @Path("/{z}/{x}/{y}")
   @GET
   @Produces("image/png")
   public BufferedImage getUserInJSON(@PathParam("z") int z, @PathParam("x") int x, @PathParam("y") int y) throws
           JsonProcessingException {
       BufferedImage img;
       byte [] tilesByte = TileDB.getTile(z,x,y);
       ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(tilesByte);
       try {
           img = ImageIO.read(byteArrayInputStream);
           return img;
       } catch (IOException e){
           e.printStackTrace();
       }
       return null;
   }
}
