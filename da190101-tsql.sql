CREATE PROCEDURE [dbo].[SP_FINAL_PRICE]
	@IdOrd int,
	@TimeSent datetime,
	@FinalPrice decimal(10, 3) output
AS
BEGIN
	declare @IdItem int
	declare @ItemCursor cursor
	declare @Quantity int
	declare @Price decimal(10, 3)
	declare @Sum decimal(10, 3)
	declare @Discount int
	declare @Count int

	select @Count = count(*) from [Transaction] join [Order] on [Transaction].IdOrd = [Order].IdOrd
                join BuyerTransaction on [Transaction].IdTra = BuyerTransaction.IdTra where Ammount >= 10000 and
                [Transaction].IdOrd != @IdOrd and DATEDIFF(day, ExecutionTime, @TimeSent) <= 30

	set @Sum = 0

	set @ItemCursor = cursor for
	select IdItem
	from Item
	where IdOrd = @IdOrd

	open @ItemCursor
	fetch next from @ItemCursor
	into @IdItem

	while @@FETCH_STATUS = 0
	Begin
		select @Quantity = Item.Quantity, @Price = Price, @Discount = Discount
		from Item Join Article on Item.IdArt = Article.IdArt join Shop on Article.IdShop = Shop.IdShop
		where IdItem = @IdItem

		set @Sum = @Sum + @Quantity * @Price * (100 - @Discount) / 100

		fetch next from @ItemCursor
		into @IdItem
	End

	close @ItemCursor
	deallocate @ItemCursor

	if (@Count > 0) 
		set @Sum = @Sum * 0.98

	set @FinalPrice = @Sum
	
END
GO

CREATE TRIGGER [dbo].[TR_TRANSFER_MONEY_TO_SHOPS]
   ON [dbo].[Order]
   AFTER update
AS 
BEGIN
	declare @ShopCursor cursor
	declare @IdShop int
	declare @ShopProfit decimal(10, 3)
	declare @OldState varchar(100)
	declare @NewState varchar(100)
	declare @IdOrd int
	declare @receivedTime datetime
	declare @IdTra int

	select @OldState = [Status] from deleted 
	select @NewState = [Status] from inserted
	select @IdOrd = IdOrd from inserted
	select @receivedTime = TimeReceived from inserted
	
	if (@OldState = 'sent' and @NewState = 'arrived')
	begin

		set @ShopCursor = cursor for
		select distinct(Shop.IdShop) from Shop join Article on Shop.IdShop = Article.IdShop join
		Item on Item.IdArt = Article.IdArt join inserted I on Item.IdOrd = I.IdOrd

		open @ShopCursor
		fetch next from @ShopCursor
		into @IdShop

		while @@FETCH_STATUS = 0
		Begin
			select @ShopProfit = sum (Item.Quantity * Price * (100 - Discount) / 100)
			from Item Join Article on Item.IdArt = Article.IdArt join inserted I on Item.IdOrd = I.IdOrd
				join Shop on Shop.IdShop = Article.IdShop
			where Article.IdShop = @IdShop

			insert into [dbo].[Transaction] (Ammount, IdOrd, ExecutionTime)
			values (@ShopProfit * 0.95, @IdOrd, @receivedTime)

			set @IdTra = @@IDENTITY

			insert into ShopTransaction (IdTra, IdShop)
			values (@IdTra, @IdShop)

			fetch next from @ShopCursor
			into @IdShop
		End

		close @ShopCursor
		deallocate @ShopCursor
	end
END
