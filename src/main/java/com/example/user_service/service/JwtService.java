package com.example.user_service.service;

import io.jsonwebtoken.Claims;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.crypto.SecretKey;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;

@Service
public class JwtService {

//Decode the token & extarct the payload
    public Claims extractAllClaims(String token)
    {
        return  Jwts.parser()
                .verifyWith(getSignKey())
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }
    //read expiration date from Claims
    public Date extractExpirationTime(String token)
    {
        return extractAllClaims(token).getExpiration();
    }
    // calculate expiration in milliseconds safely
    public long getRemainingTime(String token) {
        try {
            Date expiration = extractExpirationTime(token);
            long remaining = expiration.getTime() - System.currentTimeMillis();
            return Math.max(remaining, 0);
        } catch (io.jsonwebtoken.ExpiredJwtException e) {
            // Already expired, no remaining TTL needed in Redis
            return 0;
        } catch (io.jsonwebtoken.JwtException | IllegalArgumentException e) {
            // Invalid or malformed token
            return 0;
        }
    }
    @Value("${jwt.secret}")
    private String secretKey;

    public String generateToken(Long userId, String email, String role) {
        Map<String, Object> claims = new HashMap<>();
        claims.put("userId", userId);
        claims.put("email", email);
        claims.put("role", role);
        claims.put("roles", role);
       return Jwts.builder()
               .claims(claims)
               .subject(email)
               .issuedAt(new Date(System.currentTimeMillis()))
               .expiration(new Date(System.currentTimeMillis()+(1000*60*60*24)))
        .signWith(getSignKey(), Jwts.SIG.HS256)
            .compact();



    }

    private SecretKey getSignKey() {
        byte[] keyBytes = Decoders.BASE64.decode(secretKey);
        return Keys.hmacShaKeyFor(keyBytes);
    }

}
