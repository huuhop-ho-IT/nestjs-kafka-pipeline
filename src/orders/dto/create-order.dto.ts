import { ApiProperty } from '@nestjs/swagger';
import { IsString, IsNumber, IsPositive, Min, IsNotEmpty } from 'class-validator';

export class CreateOrderDto {
  @ApiProperty({ example: 'MacBook Pro 16"', description: 'Product name' })
  @IsString()
  @IsNotEmpty()
  product: string;

  @ApiProperty({ example: 2, description: 'Order quantity' })
  @IsNumber()
  @IsPositive()
  quantity: number;

  @ApiProperty({ example: 2499.99, description: 'Unit price in USD' })
  @IsNumber()
  @Min(0)
  price: number;

  @ApiProperty({ example: 'customer-abc-123', description: 'Customer ID' })
  @IsString()
  @IsNotEmpty()
  customerId: string;
}
