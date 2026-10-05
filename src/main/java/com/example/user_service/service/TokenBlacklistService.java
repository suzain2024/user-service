package com.example.user_service.service;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class TokenBlacklistService {

    private static final String BLACKLIST_PREFIX = "blacklist:";
    private final StringRedisTemplate redisTemplate;
    private final JwtService jwtService;

    // Spring Boot auto-injects StringRedisTemplate and JwtService
    public TokenBlacklistService(StringRedisTemplate redisTemplate, JwtService jwtService) {
        this.redisTemplate = redisTemplate;
        this.jwtService = jwtService;
    }

    // 1. Store token in Redis with dynamic TTL
    public void blacklistToken(String token) {
        if (token == null || token.isBlank()) {
            return;
        }
        long remainingTtl = jwtService.getRemainingTime(token);
        if (remainingTtl > 0) {
            String redisKey = BLACKLIST_PREFIX + token;
            redisTemplate.opsForValue().set(redisKey, "revoked", remainingTtl, TimeUnit.MILLISECONDS);
        }
    }

    // 2. Check if token exists in Redis blacklist
    public boolean isTokenBlacklisted(String token) {
        if (token == null || token.isBlank()) {
            return false;
        }
        String redisKey = BLACKLIST_PREFIX + token;
        return Boolean.TRUE.equals(redisTemplate.hasKey(redisKey));
    }
}
