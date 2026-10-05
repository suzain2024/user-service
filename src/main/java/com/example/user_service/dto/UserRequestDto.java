

package com.example.user_service.dto;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
@Data
public class UserRequestDto {
   @NotBlank(message="name can't be empty")
    private String name;
    @NotBlank(message="email can't be empty")
    @Email(message="Invalid email")
    private String email;
    @NotNull(message="[password] can't be null")
    private String password;
    private String role;
}
