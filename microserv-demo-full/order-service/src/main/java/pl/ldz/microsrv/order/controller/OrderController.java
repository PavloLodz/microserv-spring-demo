package pl.ldz.microsrv.order.controller;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

  @PostMapping
  public String create() {
    return "order created";
  }
}
