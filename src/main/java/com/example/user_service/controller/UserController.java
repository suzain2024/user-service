package com.example.user_service.controller;

import com.example.user_service.dto.AuthResponseDto;
import com.example.user_service.dto.LoginRequestDto;
import com.example.user_service.dto.UserRequestDto;
import com.example.user_service.dto.UserResponseDto;
import com.example.user_service.service.UserService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping({"/api/users", "/api/user"})
public class UserController {
    private final UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    @PostMapping("/logout")
    public ResponseEntity<Map<String, String>> logout(@RequestHeader(value = "Authorization", required = false) String authHeader) {
        if (authHeader == null || !authHeader.startsWith("Bearer ") || authHeader.length() <= 7) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "Missing or invalid Authorization header"));
        }
        String token = authHeader.substring(7).trim();
        userService.logout(token);
        return ResponseEntity.ok(Map.of("message", "Logged out successfully"));
    }
    @PostMapping({"/register", "/create"})
    public ResponseEntity<UserResponseDto> createUser(@Valid @RequestBody UserRequestDto user) {
        UserResponseDto createdUser
        = userService.createUser(user);
        return new ResponseEntity<>(createdUser, HttpStatus.CREATED);
    }

    @PostMapping("/login")
    public ResponseEntity<AuthResponseDto> login(@RequestBody LoginRequestDto loginRequest) {
        AuthResponseDto response = userService.login(loginRequest);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/get/{id}")
    public ResponseEntity<UserResponseDto> getUserDetailById(@PathVariable Long id) {
        UserResponseDto user = userService.getUserById(id);
        return ResponseEntity.ok(user);
    }

    @GetMapping({"/all", "/get"})
    public ResponseEntity<List<UserResponseDto>> getAllUsers() {
        List<UserResponseDto> users = userService.getAllUsers();
        return ResponseEntity.ok(users);
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<Void> deleteUser(@PathVariable Long id) {
        userService.deleteUser(id);
        return ResponseEntity.noContent().build();
    }

    @PutMapping("/update/{id}")
    public ResponseEntity<UserResponseDto> updateUser(@PathVariable Long id, @Valid @RequestBody UserRequestDto user) {
        UserResponseDto updatedUser = userService.updateUserById(id, user);
        return ResponseEntity.ok(updatedUser);
    }
}
